package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	CMD_FAST = 1 << iota
	CMD_SENTINEL
)

type Argument struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

type CommandInfo struct {
	Summary       string     `json:"summary"`
	Complexity    string     `json:"complexity"`
	Group         string     `json:"group"`
	Since         string     `json:"since"`
	Arity         int        `json:"arity"`
	FunctionName  string     `json:"function"`
	CommandFlags  []string   `json:"command_flags"`
	AclCategories []string   `json:"acl_categories"`
	CommandTips   []string   `json:"command_tips"`
	Arguments     []Argument `json:"arguments"`
}

type CommandRequest struct {
	Cmd      string
	Args     []interface{}
	Response chan<- []byte
}

type RedisCommand struct {
	Name     string
	Function func(server *RedisServer, cmd string, args []interface{}) []byte
	Group    string
	MinArgs  int
	CmdFlags int
	Category string
}

type RedisServer struct {
	Storage     map[string]string
	Expirations map[string]time.Time
}

var redisCommandTable map[string]RedisCommand

func main() {
	// load all redis commands with json files into RedisCommandTable map
	redisCommandTable = loadCommandsFromJSON("app/commands")
	redisServer := &RedisServer{
		Storage:     make(map[string]string),
		Expirations: make(map[string]time.Time),
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(redisServer, conn)
	}
}

func loadCommandsFromJSON(dir string) map[string]RedisCommand {
	commandTable := make(map[string]RedisCommand)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println("Error reading commands directory:", err)
		os.Exit(1)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			data, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil {
				fmt.Println("Error reading JSON file:", err)
				continue
			}

			var commands map[string]CommandInfo
			err = json.Unmarshal(data, &commands)
			if err != nil {
				fmt.Println("Error parsing JSON file:", err)
				continue
			}

			for cmdName, info := range commands {
				cmd := RedisCommand{
					Name:     cmdName,
					Function: getFunctionByName(info.FunctionName),
					Group:    info.Group,
					MinArgs:  info.Arity,
					Category: strings.Join(info.AclCategories, ","),
				}

				// Add command flags
				cmdFlags := 0
				for _, flag := range info.CommandFlags {
					switch flag {
					case "FAST":
						cmdFlags |= CMD_FAST
					case "SENTINEL":
						cmdFlags |= CMD_SENTINEL
					}
				}
				cmd.CmdFlags = cmdFlags

				commandTable[cmdName] = cmd
			}
		}
	}

	return commandTable
}

func getFunctionByName(name string) func(server *RedisServer, cmd string, args []interface{}) []byte {
	switch name {
	case "pingCommand":
		return handlePingCommand
	case "echoCommand":
		return handleEchoCommand
	case "handleSetCommand":
		return (*RedisServer).handleSetCommand
	case "handleGetCommand":
		return (*RedisServer).handleGetCommand
	default:
		return nil
	}
}

func handleConnection(server *RedisServer, conn net.Conn) {
	defer conn.Close()

	commandChan := make(chan CommandRequest)
	go handleCommands(server, conn, commandChan)

	reader := bufio.NewReader(conn)
	for {
		cmd, args, err := readCommand(reader)
		if err != nil {
			fmt.Println("Error reading from connection: ", err)
			return
		}

		if cmd == "" {
			continue
		}

		responseChan := make(chan []byte)
		commandChan <- CommandRequest{Cmd: cmd, Args: args, Response: responseChan}
		response := <-responseChan
		conn.Write(response)

		fmt.Printf("Command: %s, Arguments: %v\n", cmd, args)
	}
}

func handleCommands(server *RedisServer, conn net.Conn, commandChan <-chan CommandRequest) {
	for commandRequest := range commandChan {
		cmd := commandRequest.Cmd
		args := commandRequest.Args

		if command, ok := redisCommandTable[cmd]; ok {
			response := command.Function(server, cmd, args)
			commandRequest.Response <- response
		} else {
			response := []byte(fmt.Sprintf("-ERR Unknown command: %s\r\n", cmd))
			commandRequest.Response <- response
		}
	}
}

func readCommand(reader *bufio.Reader) (string, []interface{}, error) {
	prefix, err := reader.Peek(1)
	if err != nil {
		return "", nil, err
	}

	if prefix[0] != '*' {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", nil, err
		}

		return strings.ToUpper(strings.TrimSpace(line)), nil, nil
	}

	resp, err := readRESP(reader)
	if err != nil {
		return "", nil, err
	}

	respArray, ok := resp.([]interface{})
	if !ok || len(respArray) == 0 {
		return "", nil, nil
	}

	cmd, ok := respArray[0].(string)
	if !ok {
		return "", nil, fmt.Errorf("invalid command: %v", respArray[0])
	}

	return strings.ToUpper(cmd), respArray[1:], nil
}

// simple RESP reader
func readRESP(reader *bufio.Reader) (interface{}, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+', '-', ':':
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strings.TrimSpace(line), nil
	case '$':
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		size, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, err
		}

		if size == -1 {
			return nil, nil
		}

		buf := make([]byte, size+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		return string(buf[:size]), nil
	case '*':
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		count, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, err
		}

		array := make([]interface{}, count)
		for i := 0; i < count; i++ {
			elem, err := readRESP(reader)
			if err != nil {
				return nil, err
			}

			array[i] = elem
		}

		return array, nil
	default:
		return nil, fmt.Errorf("invalid RESP prefix: %q", prefix)
	}
}

func handlePingCommand(server *RedisServer, cmd string, args []interface{}) []byte {
	if len(args) > 1 {
		return addReplyErrorArity()
	}

	if len(args) == 0 {
		return addReply(redisCommandTable[cmd])
	} else {
		return addReplyBulk(args)
	}
}

func handleEchoCommand(server *RedisServer, cmd string, args []interface{}) []byte {
	if len(args) != 1 {
		return addReplyErrorArity()
	}

	arg, ok := args[0].(string)
	if !ok {
		return []byte("-ERR Invalid argument type\r\n")
	}

	return addReplyBulk([]interface{}{arg})
}

func (server *RedisServer) handleSetCommand(cmd string, args []interface{}) []byte {
	if len(args) != 2 && len(args) != 4 {
		return addReplyErrorArity()
	}

	key, ok := args[0].(string)
	if !ok {
		return []byte("-ERR Invalid key type\r\n")
	}

	value, ok := args[1].(string)
	if !ok {
		return []byte("-ERR Invalid value type\r\n")
	}

	if len(args) == 4 {
		expiryOption, ok := args[2].(string)
		if !ok || strings.ToUpper(expiryOption) != "PX" {
			return []byte("-ERR Invalid expiry option\r\n")
		}

		expiry, ok := args[3].(string)
		if !ok {
			return []byte("-ERR Invalid expiry type\r\n")
		}

		expiryInt, err := strconv.Atoi(expiry)
		if err != nil {
			return []byte("-ERR Invalid expiry value\r\n")
		}

		server.Storage[key] = value
		server.Expirations[key] = time.Now().Add(time.Duration(expiryInt) * time.Millisecond)
	} else {
		server.Storage[key] = value
		delete(server.Expirations, key)
	}

	return []byte("+OK\r\n")
}

func (server *RedisServer) handleGetCommand(cmd string, args []interface{}) []byte {
	if len(args) != 1 {
		return addReplyErrorArity()
	}

	key, ok := args[0].(string)
	if !ok {
		return []byte("-ERR Invalid key type\r\n")
	}

	// Check if the key has expired
	if expiration, exists := server.Expirations[key]; exists && time.Now().After(expiration) {
		delete(server.Storage, key)
		delete(server.Expirations, key)
		return []byte("$-1\r\n")
	}

	value, ok := server.Storage[key]
	if !ok {
		return []byte("$-1\r\n")
	}

	return addReplyBulk([]interface{}{value})
}

func addReplyErrorArity() []byte {
	return []byte("-ERR wrong number of arguments\r\n")
}

func addReply(command RedisCommand) []byte {
	switch command.Name {
	case "PING":
		return []byte("+PONG\r\n")
	default:
		errMsg := fmt.Sprintf("-ERR Unknown command %s\r\n", command.Name)
		return []byte(errMsg)
	}
}

func addReplyBulk(args []interface{}) []byte {
	if len(args) == 0 {
		return []byte("+\r\n")
	} else {
		reply := bytes.Buffer{}
		for _, arg := range args {
			switch value := arg.(type) {
			case string:
				reply.WriteString(fmt.Sprintf("$%d\r\n", len(value)))
				reply.WriteString(fmt.Sprintf("%s\r\n", value))
			case []interface{}:
				return addReplyBulk(value)
			default:
				errMsg := fmt.Sprintf("-ERR Unknown argument type %T\r\n", value)
				return []byte(errMsg)
			}
		}
		return reply.Bytes()
	}
}
