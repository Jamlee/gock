package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sync"

	"github.com/go-zeromq/zmq4"
	"github.com/pkg/errors"
)

type jupyterKernelConfig struct {
	Argv        []string          `json:"argv"`
	DisplayName string            `json:"display_name"`
	Language    string            `json:"language"`
	Env         map[string]string `json:"env"`
}

// 存储连接配置
type connectionInfo struct {
	SignatureScheme string `json:"signature_scheme"`
	Transport       string `json:"transport"`
	StdinPort       int    `json:"stdin_port"`
	ControlPort     int    `json:"control_port"`
	IOPubPort       int    `json:"iopub_port"`
	HBPort          int    `json:"hb_port"`
	ShellPort       int    `json:"shell_port"`
	Key             string `json:"key"`
	IP              string `json:"ip"`
}

type SyncSocket struct {
	Socket zmq4.Socket
	Lock   sync.Mutex
}

// RunLocked locks socket and runs `fn`.
func (s *SyncSocket) RunLocked(fn func(socket zmq4.Socket) error) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	return fn(s.Socket)
}

type SocketGroup struct {
	ShellSocket   SyncSocket
	ControlSocket SyncSocket
	StdinSocket   SyncSocket
	IOPubSocket   SyncSocket
	HBSocket      SyncSocket
	Key           []byte
}

type Kernel struct {
	// 连接文件路径
	ConnectionFilePath string
	shellChan          chan zmq4.Msg
	controlChan        chan zmq4.Msg
	ioChan             chan zmq4.Msg
	stdinChan          chan zmq4.Msg
	heartbeatChan      chan zmq4.Msg

	// 管理底层zmq连接
	sockets *SocketGroup

	// 接受停止 kernel 执行消息
	stop chan struct{}
}

func NewKernel() *Kernel {
	return &Kernel{
		ConnectionFilePath: "conn.json",
		shellChan:          make(chan zmq4.Msg),
		controlChan:        make(chan zmq4.Msg),
		ioChan:             make(chan zmq4.Msg),
		stdinChan:          make(chan zmq4.Msg),
		heartbeatChan:      make(chan zmq4.Msg),
		stop:               make(chan struct{}),
	}
}

func (k *Kernel) setupSockets() error {
	var connInfo connectionInfo
	connData, err := os.ReadFile(k.ConnectionFilePath)
	if err != nil {
		return errors.WithMessagef(err, "failed to open connection file %s", k.ConnectionFilePath)
	}
	if err = json.Unmarshal(connData, &connInfo); err != nil {
		return errors.WithMessagef(err, "failed to read from connection file %s", k.ConnectionFilePath)
	}

	ctx := context.Background()
	sg := &SocketGroup{
		Key:           []byte(connInfo.Key),
		ShellSocket:   SyncSocket{Socket: zmq4.NewRouter(ctx)},
		ControlSocket: SyncSocket{Socket: zmq4.NewRouter(ctx)},
		StdinSocket:   SyncSocket{Socket: zmq4.NewRouter(ctx)},
		IOPubSocket:   SyncSocket{Socket: zmq4.NewPub(ctx)},
		HBSocket:      SyncSocket{Socket: zmq4.NewRep(ctx)},
	}
	// 函数生成连接串
	var addrFn func(portNum int) string
	switch connInfo.Transport {
	case "tcp":
		addrFn = func(portNum int) string {
			return fmt.Sprintf("tcp://%s:%d", connInfo.IP, portNum)
		}
	case "ipc":
		addrFn = func(portNum int) string {
			return fmt.Sprintf("ipc://%s-%d", connInfo.IP, portNum)
		}
	}

	// 循环对所有 sockets 启动监听
	portNums := []int{connInfo.ShellPort, connInfo.ControlPort, connInfo.StdinPort,
		connInfo.IOPubPort, connInfo.HBPort}
	sockets := []*SyncSocket{&sg.ShellSocket, &sg.ControlSocket, &sg.StdinSocket,
		&sg.IOPubSocket, &sg.HBSocket}
	socketName := []string{"shell-socket", "control-socket", "stdin-socket",
		"iopub-socket", "heartbeat-socket"}
	for ii, portNum := range portNums {
		address := addrFn(portNum)
		err = sockets[ii].Socket.Listen(address)
		if err != nil {
			return errors.WithMessagef(err, fmt.Sprintf("failed to listen on %s", socketName[ii]))
		}
	}
	k.sockets = sg

	// 接收 zeromq 消息
	poll := func(msgChan chan zmq4.Msg, sck zmq4.Socket) {
		go func() {
			defer close(msgChan)
			var msg zmq4.Msg
			for {
				zmqMsg, err := sck.Recv()
				if err != nil {
					msg = zmqMsg
				} else {
					msg = zmqMsg
				}
				select {
				case msgChan <- msg:
				case <-k.stop:
					return
				}
			}
		}()
	}

	poll(k.shellChan, k.sockets.ShellSocket.Socket)
	poll(k.ioChan, k.sockets.StdinSocket.Socket)
	poll(k.stdinChan, k.sockets.StdinSocket.Socket)
	poll(k.controlChan, k.sockets.ControlSocket.Socket)
	poll(k.heartbeatChan, k.sockets.ControlSocket.Socket)

	return nil
}

func (k *Kernel) Start() error {
	// 设置监听端口
	err := k.setupSockets()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	// poll 创建 goroutine 执行独立端口监听
	poll := func(msg <-chan zmq4.Msg, fn func(msg zmq4.Msg)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-k.stop:
					return
				case m := <-msg:
					fn(m)
				}
			}
		}()
	}

	// 设置消息处理函数
	poll(k.shellChan, func(msg zmq4.Msg) {
		fmt.Println(msg)
	})
	poll(k.ioChan, func(msg zmq4.Msg) {
		fmt.Println(msg)
	})
	poll(k.stdinChan, func(msg zmq4.Msg) {
		fmt.Println(msg)
	})
	poll(k.controlChan, func(msg zmq4.Msg) {
		fmt.Println(msg)
	})
	poll(k.heartbeatChan, func(msg zmq4.Msg) {
		err = k.sockets.HBSocket.RunLocked(func(echo zmq4.Socket) error {
			if err := echo.Send(msg); err != nil {
				errors.WithMessagef(err, "error sending heartbeat pong %q", msg.String())
				return err
			}
			return nil
		})
	})

	wg.Wait()
	return nil
}

func (k *Kernel) Install() error {
	config := jupyterKernelConfig{
		Argv:        []string{os.Args[0], "--kernel", "{connection_file}"},
		DisplayName: "gock)",
		Language:    "go",
		Env:         make(map[string]string),
	}
	home := os.Getenv("HOME")
	configDir := path.Join(home, ".local/share/jupyter/kernels/gock")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return errors.WithMessagef(err, "failed to create configuration directory %q", configDir)
	}
	configPath := path.Join(configDir, "kernel.json")
	f, err := os.Create(configPath)
	if err != nil {
		return errors.WithMessagef(err, "failed to create configuration file %q", configPath)
	}
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(&config); err != nil {
		if err != nil {
			return errors.WithMessagef(err, "failed to write configuration file %q", configPath)
		}
	}
	if err := f.Close(); err != nil {
		if err != nil {
			return errors.WithMessagef(err, "failed to write configuration file %q", configPath)
		}
	}

	log.Printf("Go (gonb) kernel configuration installed in %q.\n", configPath)
	return nil
}

func (k *Kernel) HandleInterrupt() {
	sigintC := make(chan os.Signal, 1)
	signal.Notify(sigintC, os.Interrupt)
	go func() {
		// At exit reset notification.
		defer func() {
			signal.Reset(os.Interrupt)
			sigintC = nil
		}()
		for {
			select {
			case <-sigintC:
				log.Printf("INTERRUPT received")
				os.Exit(0)
			case <-k.stop:
				return // kernel stopped.
			}
		}
	}()
}

func (k *Kernel) Stop() {
	close(k.stop)
}

var (
	flagInstall = flag.Bool("install", false, "Install kernel in local config, and make it available in Jupyter")
	flagKernel  = flag.String("kernel", "", "Run kernel using given path for the `connection_file` provided by Jupyter client")
)

func main() {
	flag.Parse()
	kernel := NewKernel()
	// 安装内核
	if *flagInstall {
		err := kernel.Install()
		if err != nil {
			log.Fatalf("Installation failed: %+v\n", err)
		}
		return
	}
	if *flagKernel == "" {
		fmt.Fprintf(os.Stderr, "Use either --install to install the kernel, or if started by Jupyter the flag --kernel must be provided.\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.Println("started gock kernel")
	kernel.ConnectionFilePath = *flagKernel
	kernel.HandleInterrupt()
	err := kernel.Start()
	if err != nil {
		panic(err)
	}
}
