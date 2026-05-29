package ssh

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	extractDirPerm  = 0o755
	extractFilePerm = 0o644
)

// extractTarStream reads a tar stream from r and writes its entries
// under dst. Extraction is fail-closed: it tolerates exactly one
// anomaly, self-referential hardlinks (an entry whose link target is
// itself, which macOS bsdtar emits for some Antigravity data and which
// carries no content), and treats every other problem as fatal so a
// truncated or corrupt transfer can never masquerade as a successful
// sync. Unexpected EOF, bad headers, paths escaping dst, and
// write/short-read errors all return an error. Returns the number of
// self-referential hardlinks skipped.
func extractTarStream(
	ctx context.Context, r io.Reader, dst string,
) (int, error) {
	tr := tar.NewReader(r)
	skipped := 0
	for {
		if err := ctx.Err(); err != nil {
			return skipped, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return skipped, nil
		}
		if err != nil {
			return skipped, fmt.Errorf("read tar entry: %w", err)
		}
		selfLink, err := extractEntry(tr, dst, hdr)
		if err != nil {
			return skipped, err
		}
		if selfLink {
			skipped++
		}
	}
}

// extractEntry writes a single tar entry under dst. It reports whether
// the entry was a self-referential hardlink (skipped, no error).
func extractEntry(
	tr *tar.Reader, dst string, hdr *tar.Header,
) (bool, error) {
	target, err := safeJoin(dst, hdr.Name)
	if err != nil {
		return false, err
	}
	switch hdr.Typeflag {
	case tar.TypeDir:
		return false, mkdirAll(target, hdr.Name)
	case tar.TypeReg:
		return false, writeRegular(target, tr, hdr)
	case tar.TypeSymlink:
		// Symlinks are not session data, and a symlink restored from
		// an archive can redirect later writes outside the extraction
		// dir. Any regular file a symlink might alias is extracted on
		// its own, so skip symlinks entirely.
		return false, nil
	case tar.TypeLink:
		return writeHardlink(dst, target, hdr)
	default:
		// Char/block/fifo and similar special files do not appear
		// in agent session directories; there is no content to lose
		// by ignoring them.
		return false, nil
	}
}

// safeJoin resolves name against dst and rejects any path that escapes
// dst (via "..", an absolute component, or symlink-free traversal).
func safeJoin(dst, name string) (string, error) {
	target := filepath.Join(dst, filepath.FromSlash(name))
	if !within(dst, target) {
		return "", fmt.Errorf(
			"tar entry %q escapes extraction dir", name,
		)
	}
	return target, nil
}

// within reports whether p is dst itself or lies inside dst.
func within(dst, p string) bool {
	rel, err := filepath.Rel(dst, p)
	if err != nil {
		return false
	}
	if rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func mkdirAll(path, name string) error {
	if err := os.MkdirAll(path, extractDirPerm); err != nil {
		return fmt.Errorf("mkdir %q: %w", name, err)
	}
	return nil
}

// writeRegular extracts a regular file, failing on a short read so a
// truncated stream cannot leave a half-written file behind. On any
// failure the partial file is removed, so an aborted entry never looks
// complete.
func writeRegular(
	target string, tr io.Reader, hdr *tar.Header,
) (err error) {
	if e := mkdirAll(filepath.Dir(target), hdr.Name); e != nil {
		return e
	}
	f, e := os.OpenFile(
		target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, extractFilePerm,
	)
	if e != nil {
		return fmt.Errorf("create %q: %w", hdr.Name, e)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(target)
		}
	}()
	n, copyErr := io.Copy(f, tr)
	closeErr := f.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return fmt.Errorf("write %q: %w", hdr.Name, copyErr)
	}
	if n != hdr.Size {
		return fmt.Errorf(
			"short write %q: got %d of %d bytes",
			hdr.Name, n, hdr.Size,
		)
	}
	// Restore the archived mtime: the incremental skip cache keys on
	// (path, mtime), so files must keep their remote mtime across
	// syncs or nothing is ever skipped. Best-effort: a failure only
	// forces a redundant resync, never data loss, so it must not
	// discard an otherwise complete extraction.
	if !hdr.ModTime.IsZero() {
		_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
	}
	return nil
}

// writeHardlink recreates a hardlink. A self-referential hardlink
// (target equals the entry itself) carries no content and is skipped;
// the bool return reports that case. Any other failure is fatal.
func writeHardlink(
	dst, target string, hdr *tar.Header,
) (bool, error) {
	linkTarget, err := safeJoin(dst, hdr.Linkname)
	if err != nil {
		return false, err
	}
	if linkTarget == target {
		return true, nil
	}
	if err := mkdirAll(filepath.Dir(target), hdr.Name); err != nil {
		return false, err
	}
	if err := os.Link(linkTarget, target); err != nil {
		return false, fmt.Errorf(
			"hardlink %q -> %q: %w", hdr.Name, hdr.Linkname, err,
		)
	}
	return false, nil
}
