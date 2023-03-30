package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	CMD_FAST = 1 << iota
	CMD_SENTINEL
)

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

type Argument struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

type RedisCommand struct {
	Name     string
	Function func(net.Conn, string, []interface{})
	Group    string
	MinArgs  int
	CmdFlags int
	Category string
}

var redisCommandTable map[string]RedisCommand

func main() {
	// load all redis commands with json files into RedisCommandTable map
	redisCommandTable = loadCommandsFromJSON("app/commands")

	// log if converting correct
	if pingCmd, ok := redisCommandTable["PING"]; ok {
		fmt.Printf("Name: %s\nFunction: %T\nGroup: %s\nMinArgs: %d\nCmdFlags: %d\nCategory: %s\n",
			pingCmd.Name, pingCmd.Function, pingCmd.Group, pingCmd.MinArgs, pingCmd.CmdFlags, pingCmd.Category)
	} else {
		fmt.Println("The 'PING' command was not found in the command table.")
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

		go handleConnection(conn)
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

func getFunctionByName(name string) func(conn net.Conn, cmd string, args []interface{}) {
	switch name {
	case "pingCommand":
		return pingCommand
	default:
		return nil
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

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

		switch cmd {
		case "PING":
			pingCommand(conn, cmd, args)
		default:
			fmt.Fprintf(conn, "Unknown command: %s\n", cmd)
		}

		fmt.Printf("Command: %s, Arguments: %v\n", cmd, args)
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

func pingCommand(conn net.Conn, cmd string, args []interface{}) {
	if len(args) > 1 {
		addReplyErrorArity(conn)
		return
	}

	if len(args) == 0 {
		addReply(conn, redisCommandTable[cmd])
	} else {
		addReplyBulk(conn, args)
	}
}

func addReplyErrorArity(conn net.Conn) {
	conn.Write([]byte("-ERR wrong number of arguments\r\n"))
}

func addReply(conn net.Conn, command RedisCommand) {
	switch command.Name {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	default:
		errMsg := fmt.Sprintf("-ERR Unknown command %s\r\n", command.Name)
		conn.Write([]byte(errMsg))
	}
}

func addReplyBulk(conn net.Conn, args []interface{}) {
	if len(args) == 0 {
		conn.Write([]byte("+\r\n"))
	} else {
		for _, arg := range args {
			switch value := arg.(type) {
			case string:
				conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(value))))
				conn.Write([]byte(fmt.Sprintf("%s\r\n", value)))
			case []interface{}:
				addReplyBulk(conn, value)
			default:
				errMsg := fmt.Sprintf("-ERR Unknown argument type %T\r\n", value)
				conn.Write([]byte(errMsg))
			}
		}
	}
}
