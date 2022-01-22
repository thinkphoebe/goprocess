package goprocess

import (
	"bufio"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"

	log "github.com/thinkphoebe/golog"
)

type SubprocessTask struct {
	Exe    string
	Args   []string
	TaskId string

	KeepAlivePrefix string
	Timeout         int64

	CbLogJson       func(level log.LogLevel, j log.Json)
	ShowMemInterval int

	cmd         *exec.Cmd
	chKeepAlive chan struct{}
	pid         int
}

func (self *SubprocessTask) Start() (int, error) {
	var stdoutIO io.ReadCloser
	var stderrIO io.ReadCloser
	var err error = nil

	// 先创建一个goroutine检查keepalive，避免在程序启动、运行中block住
	self.chKeepAlive = make(chan struct{}, 100)
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		tickChan := ticker.C
		lastKeepAlive := time.Now().Unix()

		if self.ShowMemInterval < 1 {
			self.ShowMemInterval = 3600
		}
		tickerMem := time.NewTicker(time.Second * time.Duration(self.ShowMemInterval))
		tickChanMem := tickerMem.C

		for {
			select {
			case _, ok := <-self.chKeepAlive:
				lastKeepAlive = time.Now().Unix()
				if !ok {
					log.Infof("[%s] chKeepAlive closed, exit check", self.TaskId)
					ticker.Stop()
					tickerMem.Stop()
					return
				}
			case <-tickChan:
				if self.Timeout > 0 && time.Now().Unix()-lastKeepAlive > self.Timeout {
					log.Errorf("[%s] subprocess blocked, now:%d, last keepalive:%d, pid:%d, kill",
						self.TaskId, time.Now().Unix(), lastKeepAlive, self.pid)
					self.Cancel()
				}
			case <-tickChanMem:
				if self.CbLogJson != nil {
					stat := ProcStat{Pid: self.pid}
					err := stat.Update()
					if err == nil {
						// rss单位：KB
						self.CbLogJson(log.LevelInfo, log.Json{"cmd": "subprocess_mem",
							"task_id": self.TaskId, "rss": stat.Rss * 4096 / 1024})
					} else {
						log.Warnf("ProcStat Update() got err:%v, pid:%d", err, self.pid)
					}
				}
			}
		}
	}()

	cmd := exec.Command(self.Exe, self.Args...)
	log.Infof("[%s] run cmd [%s][%s]", self.TaskId, self.Exe, strings.Join(self.Args, " "))

	// redirect stdout/stderr to a pipe
	if stdoutIO, err = cmd.StdoutPipe(); err != nil {
		log.Errorf("[%s] cmd.StdoutPipe got err:%s", self.TaskId, err.Error())
		goto END
	}
	if stderrIO, err = cmd.StderrPipe(); err != nil {
		log.Errorf("[%s] cmd.StderrPipe got err:%s", self.TaskId, err.Error())
		goto END
	}

	// start the child process
	if err = cmd.Start(); err != nil {
		log.Errorf("[%s] cmd.Start got err:%s", self.TaskId, err.Error())
		goto END
	}
	self.cmd = cmd
	self.pid = cmd.Process.Pid
	log.Infof("[%s] cmd.Start OK, pid %d", self.TaskId, self.pid)

	// read subprocess log and parse keepalive
	go func() {
		r := bufio.NewReader(stdoutIO)
		data, err := r.ReadBytes('\n')
		for ; err == nil; data, err = r.ReadBytes('\n') {
			str := strings.TrimSpace(string(data))
			log.Infof("[%s][%d][stdout] %s", self.TaskId, self.pid, str)
		}
		log.Infof("[%s][%d] stdout exit[%s]!", self.TaskId, self.pid, err.Error())
	}()
	go func() {
		r := bufio.NewReader(stderrIO)
		data, err := r.ReadBytes('\n')
		for ; err == nil; data, err = r.ReadBytes('\n') {
			str := strings.TrimSpace(string(data))
			if self.KeepAlivePrefix != "" && strings.HasPrefix(str, self.KeepAlivePrefix) {
				self.chKeepAlive <- struct{}{}
			} else {
				log.Infof("[%s][%d][stderr] %s", self.TaskId, self.pid, str)
			}
		}
		log.Infof("[%s][%d] stderr exit[%s]!", self.TaskId, self.pid, err.Error())
		close(self.chKeepAlive)
	}()

END:
	if err != nil {
		close(self.chKeepAlive)
	}
	return self.pid, err
}

// Start()返回err非nil时必须调用Wait()，否则会有资源泄露
func (self *SubprocessTask) Wait() (int, error) {
	log.Debugf("[%s][%d] call cmd.Wait", self.TaskId, self.pid)
	err := self.cmd.Wait()
	retCode := 0
	log.Debugf("[%s][%d] cmd.Wait complete, err:%v", self.TaskId, self.pid, err)
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			retCode = -1
			if status, done := exitErr.Sys().(syscall.WaitStatus); done {
				retCode = status.ExitStatus()
			}
		} else {
			// 不是err不是exec.ExitError类型的，说明不是来自cmd.Wait()
			// 这个时候，返回正常退出
		}
	}
	log.Infof("[%s][%d] subprocess exit, retCode:%d, err:%v", self.TaskId, self.pid, retCode, err)
	return retCode, err
}

func (self *SubprocessTask) Cancel() {
	log.Infof("[%s][%d] cmd:%v", self.TaskId, self.pid, self.cmd)
	if self.cmd != nil {
		_ = self.cmd.Process.Kill()
	} else {
		log.Warnf("[%s][%d] cmd is nil", self.TaskId, self.pid)
	}
}
