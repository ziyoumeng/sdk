package redis

import (
	"github.com/pkg/errors"
)

var Scripts []*LuaScript

//业务通过init方法调用
func AddScript(s *LuaScript) {
	Scripts = append(Scripts, s)
}

type LuaScript struct {
	scriptName string
	script     string
	sha        string
	keyCount   int
}

func NewLuaScript(name, script string, keyCount int) *LuaScript {
	return &LuaScript{
		scriptName: name,
		script:     script,
		keyCount:   keyCount,
	}
}

func (l *LuaScript) GetScriptName() string {
	return l.scriptName
}

func (l *LuaScript) GetScript() string {
	return l.script
}

func (l *LuaScript) GetKeyCount() int {
	return l.keyCount
}

func (l *LuaScript) SetScriptSha(sha string) {
	l.sha = sha
}

func (l *LuaScript) GetScriptSha() string {
	return l.sha
}

func (w *Wrapper) EvalSha(scriptName string, keys []string, args []interface{}) (interface{}, error) {
	script, ok := w.luaScript[scriptName]
	if !ok {
		return 0, errors.Errorf("not exist luaScript:%s", scriptName)
	}
	if script.GetKeyCount() != len(keys) {
		return nil, errors.New("length of keys not match with script")
	}
	values := make([]interface{}, 2+len(keys)+len(args))
	values[0] = script.GetScriptSha()
	values[1] = script.GetKeyCount()
	for i, key := range keys {
		values[i+2] = w.WithPrefix(key)
	}

	for i, arg := range args {
		values[i+2+len(keys)] = arg
	}
	//log.Debugf("len %d ,values %+v", len(values), values)
	return w.ExecRedisCommand("EVALSHA", values...)
	//return	 w.ExecRedisCommand("EVALSHA", "2895b9a31eb591b2b7a85c1ab3618c2c6bcd3b03", 2,"cm:tj:dq_bucket_1", "cm:tj:job:test1",1,
	//	"test1", "2", "1600852152000", 3, 1000, 4, 1, 5, 0, 6, "dq_bucket_1" ,7, `{"sn":"","sid":0,"mn":"","mid":0,"arg":null}`, 8, 0)
}
