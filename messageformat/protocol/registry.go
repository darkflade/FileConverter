package protocol

import "reflect"

var typeRegistry = map[reflect.Type]string{}

func GetTypeRegister() map[reflect.Type]string {
	return typeRegistry
}

func RegisterType(v any, name string) {
	typeRegistry[reflect.TypeOf(v)] = name
}

var constructorRegistry = map[string]func() any{}

func RegisterConstructor(name string, ctor func() any) {
	constructorRegistry[name] = ctor
}
