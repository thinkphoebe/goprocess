package goprocess

import (
	"errors"
	"os"
	"sync"
	"time"

	goproc "github.com/c9s/goprocinfo/linux"
	log "github.com/thinkphoebe/golog"
)

type CpuState struct {
	stopped bool
	lock    sync.RWMutex

	stepSize   int64
	average    bool
	cpuStats   []goproc.CPUStat // 不同时刻的cpu stats，环形更新
	statsProc  []float64        // 经过计算加工后的值，缓存，免得重复计算
	currTimeNs []int64          // 当前时间,单位:ns
	slide      int              // 滑动窗索引, 小于used
	used       int              // 需要使用到的项
}

const MaxCpuStatsValidDurationNs = 30 * time.Second

func (self *CpuState) CpuStateStart(windowSizeMs int64, stepSizeMs int64, average bool) error {
	self.lock.RLock()
	defer self.lock.RUnlock()

	if stepSizeMs < 1000 || windowSizeMs < 1000 || stepSizeMs > windowSizeMs {
		return errors.New("invalid stepSizeMs or windowSizeMs")
	}

	self.average = average
	self.stepSize = stepSizeMs
	self.used = int(windowSizeMs/stepSizeMs) + 1
	self.cpuStats = make([]goproc.CPUStat, self.used)
	self.statsProc = make([]float64, self.used)
	self.currTimeNs = make([]int64, self.used)
	self.slide = 0
	self.stopped = false

	go func() {
		for !self.stopped {
			self.cpuStateRecord()
			time.Sleep(time.Duration(self.stepSize) * time.Millisecond)
		}
	}()
	return nil
}

func (self *CpuState) CpuStateStop() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.stopped = true
}

func (self *CpuState) CpuStateRead() (float64, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	curTime := time.Now().UnixNano()
	if self.currTimeNs[self.slide] == 0 || !self.cpuStateIsFull() {
		// 刚开始不接任务
		return 0.0, errors.New("cpu state is not full")
	} else if curTime-self.currTimeNs[self.slide] > int64(MaxCpuStatsValidDurationNs) {
		// 最新的记录点，已经离当前时间很久远了，失效了
		return 0.0, errors.New("cpu state cur record expire")
	}

	if self.average {
		return self.getCpuOccupyPercent()
	} else {
		return clipFloat64(self.getMaxStatesProc(), 0.0, 1.0), nil
	}
}

func (self *CpuState) cpuStateNextIdx() int {
	return (self.slide + 1) % self.used
}

func (self *CpuState) cpuStatePreIdx() int {
	return (self.slide + self.used - 1) % self.used
}

func (self *CpuState) cpuStateRecord() {
	self.lock.Lock()
	defer self.lock.Unlock()

	idx := self.cpuStateNextIdx()
	curInfo, err := getCpuUsage()
	if err != nil {
		return
	}
	self.cpuStats[idx] = *curInfo
	self.currTimeNs[idx] = time.Now().UnixNano()
	self.slide = idx
	if percent, err := self.getCpuOccupyPercent(); err != nil {
		self.statsProc[idx] = 1.0
	} else {
		self.statsProc[idx] = percent
	}
}

func (self *CpuState) cpuStateIsFull() bool {
	idx := self.cpuStateNextIdx()
	if 0 != self.currTimeNs[idx] {
		return true
	}
	return false
}

func (self *CpuState) getCpuOccupyPercent() (float64, error) {
	return getCpuOccupyPercent(&self.cpuStats[self.slide], &self.cpuStats[self.cpuStatePreIdx()])
}

func (self *CpuState) getMaxStatesProc() (val float64) {
	for _, v := range self.statsProc[0:self.used] {
		if v > val {
			val = v
		}
	}
	return
}

func clipFloat64(val float64, lthres, rthres float64) float64 {
	if val < lthres {
		return lthres
	} else if val > rthres {
		return rthres
	}
	return val
}

func getTotalCpuUnits(cpuStat *goproc.CPUStat) uint64 {
	return cpuStat.Nice + cpuStat.User + cpuStat.System + cpuStat.Idle + cpuStat.IOWait + cpuStat.IRQ + cpuStat.SoftIRQ
}

func getCpuUsage() (*goproc.CPUStat, error) {
	stat, err := goproc.ReadStat("/proc/stat")
	if err != nil {
		return nil, err
	}
	cpuUsageInfo := &stat.CPUStatAll
	return cpuUsageInfo, nil
}

/**
 * [计算一定时间内的cpu平均占有率]
 * @param {*goproc.CPUStat} cpuUsageInfo		本次记录的CPU时间统计，如果为nil,函数会自动获取当前CPU时间统计
 * @param {*goproc.CPUStat} lastCpuUsageInfo	上次记录的CPU时间统计基准
 * @return {float64}	occupy	CPU平均使用率
 * @return {error} 		err		错误
 */
func getCpuOccupyPercent(cpuUsageInfo, lastCpuUsageInfo *goproc.CPUStat) (float64, error) {
	// 如果为nil，自动获取当前值
	if cpuUsageInfo == nil {
		curInfo, err := getCpuUsage()
		if err != nil {
			return 0.0, err
		}
		cpuUsageInfo = curInfo
	}

	deltaTotalUnits := getTotalCpuUnits(cpuUsageInfo) - getTotalCpuUnits(lastCpuUsageInfo)
	deltaIdleUnits := cpuUsageInfo.Idle - lastCpuUsageInfo.Idle

	if deltaTotalUnits <= 0 || deltaTotalUnits-deltaIdleUnits <= 0 {
		return 0.0, nil
	} else if deltaIdleUnits < 0 || lastCpuUsageInfo.Idle == 0 {
		return 1.0, nil
	}
	return float64(deltaTotalUnits-deltaIdleUnits) / float64(deltaTotalUnits), nil
}

func StatProc(cpuStat *CpuState, interval int, instanceId string, cbLog func(level log.LogLevel, j log.Json)) {
	if err := cpuStat.CpuStateStart(10000, 2000, true); err != nil {
		log.Fatalf("GCpuStat.CpuStateStart FAILED! %s", err.Error())
		cbLog(log.LevelCritical, log.Json{"cmd": "fatal_error", "err_msg": "CpuStateStart got err:" + err.Error()})
		return
	}

	for {
		stat := ProcStat{Pid: os.Getpid()}
		err := stat.Update()
		if err != nil {
			log.Warnf("ProcStat Update() got err:%v", err)
		}

		cpuUsage, err := cpuStat.CpuStateRead()
		if err != nil {
			log.Warnf("CpuStateRead() got err:%v", err)
		}

		// rss单位：KB
		cbLog(log.LevelInfo, log.Json{"cmd": "machine_info", "instance_id": instanceId,
			"sys_cpu_usage": int(cpuUsage * 1000), "mem_usage": stat.Rss * 4096 / 1024})
		time.Sleep(time.Second * time.Duration(interval))
	}
}
