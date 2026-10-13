//go:build linux

package worker

import (
	"log"

	linuxproc "github.com/c9s/goprocinfo/linux"
)

// Stats holds system-level metrics collected from /proc.
type Stats struct {
	MemStats  *linuxproc.MemInfo
	DiskStats *linuxproc.Disk
	CpuStats  *linuxproc.CPUStat
	LoadStats *linuxproc.LoadAvg
	TaskCount int
}

// MemTotalKb returns total memory in kilobytes.
func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.MemTotal
}

// MemAvailableKb returns available memory in kilobytes.
func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.MemAvailable
}

// MemUsedKb returns used memory in kilobytes.
func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.MemTotal - s.MemStats.MemAvailable
}

// MemUsedPercent returns memory usage as a fraction (0.0–1.0).
func (s *Stats) MemUsedPercent() float64 {
	return float64(s.MemUsedKb()) / float64(s.MemTotalKb())
}

// DiskTotal returns total disk space in bytes.
func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.All
}

// DiskFree returns free disk space in bytes.
func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

// DiskUsed returns used disk space in bytes.
func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

// CpuUsage returns the CPU utilisation as a fraction (0.0–1.0).
func (s *Stats) CpuUsage() float64 {
	idle := s.CpuStats.Idle + s.CpuStats.IOWait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System +
		s.CpuStats.IRQ + s.CpuStats.SoftIRQ + s.CpuStats.Steal
	total := idle + nonIdle
	if total == 0 {
		return 0.0
	}
	return (float64(total) - float64(idle)) / float64(total)
}

// GetStats collects all host metrics and returns a populated Stats struct.
func GetStats() *Stats {
	return &Stats{
		MemStats:  GetMemoryInfo(),
		DiskStats: GetDiskInfo(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvg(),
	}
}

// GetMemoryInfo reads /proc/meminfo.
func GetMemoryInfo() *linuxproc.MemInfo {
	memstats, err := linuxproc.ReadMemInfo("/proc/meminfo")
	if err != nil {
		log.Printf("Error reading /proc/meminfo: %v\n", err)
		return &linuxproc.MemInfo{}
	}
	return memstats
}

// GetDiskInfo reads disk usage for the root filesystem.
func GetDiskInfo() *linuxproc.Disk {
	diskstats, err := linuxproc.ReadDisk("/")
	if err != nil {
		log.Printf("Error reading disk info: %v\n", err)
		return &linuxproc.Disk{}
	}
	return diskstats
}

// GetCpuStats reads /proc/stat for aggregate CPU counters.
func GetCpuStats() *linuxproc.CPUStat {
	stats, err := linuxproc.ReadStat("/proc/stat")
	if err != nil {
		log.Printf("Error reading /proc/stat: %v\n", err)
		return &linuxproc.CPUStat{}
	}
	return &stats.CPUStatAll
}

// GetLoadAvg reads /proc/loadavg.
func GetLoadAvg() *linuxproc.LoadAvg {
	loadavg, err := linuxproc.ReadLoadAvg("/proc/loadavg")
	if err != nil {
		log.Printf("Error reading /proc/loadavg: %v\n", err)
		return &linuxproc.LoadAvg{}
	}
	return loadavg
}
