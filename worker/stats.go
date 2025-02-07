package worker

import (
	"log"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/load"
)

type Stats struct {
	//MemStats *linux.MemInfo
	// DiskStats *linux.Disk
	CpuInfo   *cpu.InfoStat
	CpuStats  *cpu.TimesStat
	LoadStats *load.AvgStat
	TaskCount int
}

// func (s *Stats) MemUsedKb() uint64 {
// 	return s.MemStats.MemTotal - s.MemStats.MemAvailable
// }

// func (s *Stats) MemUsedPercent() uint64 {
// 	return s.MemStats.MemAvailable / s.MemStats.MemTotal
// }

// func (s *Stats) MemAvailableKb() uint64 {
// 	return s.MemStats.MemAvailable
// }

// func (s *Stats) MemTotalKb() uint64 {
// 	return s.MemStats.MemTotal
// }

// func (s *Stats) DiskTotal() uint64 {
// 	return s.DiskStats.All
// }

// func (s *Stats) DiskFree() uint64 {
// 	return s.DiskStats.Free
// }

// func (s *Stats) DiskUsed() uint64 {
// 	return s.DiskStats.Used
// }

func (s *Stats) CpuUsage() float64 {
	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Println("Error of getting CPU utilization:", err)
		return 0.00
	}
	return cpuPercent[0]
}

func GetStats() *Stats {
	return &Stats{
		// MemStats: GetMemoryInfo(),
		//DiskStats: GetDiskInfo(),
		CpuInfo:   GetCpuInfo(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvg(),
	}
}

// // GetMemoryInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#MemInfo
// func GetMemoryInfo() *linux.MemInfo {
// 	memstats, err := linux.ReadMemInfo("/proc/meminfo")
// 	if err != nil {
// 		log.Printf("Error reading from /proc/meminfo")
// 		return &linux.MemInfo{}
// 	}

// 	return memstats
// }

// GetDiskInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#Disk
// func GetDiskInfo() *linux.Disk {
// 	diskstats, err := linux.ReadDisk("/")
// 	if err != nil {
// 		log.Printf("Error reading from /")
// 		return &linux.Disk{}
// 	}

// 	return diskstats
// }

func GetCpuInfo() *cpu.InfoStat {
	cpuStats, err := cpu.Info()
	if err != nil {
		log.Println("Error of CPU info:", err)
		return &cpu.InfoStat{}
	}
	return &cpuStats[0]
}

func GetCpuStats() *cpu.TimesStat {
	stats, err := cpu.Times(false)
	if err != nil {
		log.Println("Error of CPU stats:", err)
		return &cpu.TimesStat{}
	}
	return &stats[0]
}

func GetLoadAvg() *load.AvgStat {
	loadStat, err := load.Avg()
	if err != nil {
		log.Println("Error of getting CPU average load:", err)
		return &load.AvgStat{}
	}
	return loadStat
}
