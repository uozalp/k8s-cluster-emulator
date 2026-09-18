//go:build unix

package metrics

import "syscall"

// processCPUSeconds returns the CPU time this process has consumed.
func processCPUSeconds() (float64, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	secs := func(t syscall.Timeval) float64 {
		return float64(t.Sec) + float64(t.Usec)/1e6
	}
	return secs(ru.Utime) + secs(ru.Stime), true
}
