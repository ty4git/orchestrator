package worker

import (
	"log"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

type Stats struct {
	MemStats  *mem.VirtualMemoryStat
	DiskStats *disk.UsageStat
	CpuInfo   *cpu.InfoStat
	CpuStats  *cpu.TimesStat
	LoadStats *load.AvgStat
	TaskCount int
}

func (s *Stats) MemUsed() uint64 {
	return s.MemStats.Used
}

// func (s *Stats) MemUsedPercent() uint64 {
// 	return s.MemStats.MemAvailable / s.MemStats.MemTotal
// }

// func (s *Stats) MemAvailableKb() uint64 {
// 	return s.MemStats.MemAvailable
// }

func (s *Stats) MemTotal() uint64 {
	return s.MemStats.Total
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.Total
}

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
		MemStats:  GetMemoryStats(),
		DiskStats: GetDiskInfo(),
		CpuInfo:   GetCpuInfo(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvg(),
	}
}

func GetMemoryStats() *mem.VirtualMemoryStat {
	memstats, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error reading memory info: %v", err)
		return &mem.VirtualMemoryStat{}
	}

	return memstats
}

func GetDiskInfo() *disk.UsageStat {
	path := "/"
	diskUsage, err := disk.Usage(path)
	if err != nil {
		log.Printf("Error reading disk info for \"%s\": \"%v\"", path, err)
		return &disk.UsageStat{}
	}

	return diskUsage
}

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
