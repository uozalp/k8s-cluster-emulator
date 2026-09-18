//go:build !unix

package metrics

// processCPUSeconds has no portable implementation outside unix, so the
// dashboard simply omits the CPU figure there.
func processCPUSeconds() (float64, bool) { return 0, false }
