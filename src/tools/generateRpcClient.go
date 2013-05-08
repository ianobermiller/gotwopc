package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"text/template"
)

const classTemplate = `
package main

import (
	"log"
	"net/rpc"
)

type {{.Name}}Client struct {
	host      string
	rpcClient *rpc.Client
}

func New{{.Name}}Client(host string) *{{.Name}}Client {
	client := &{{.Name}}Client{host, nil}
	client.tryConnect()
	return client
}

func (c *{{.Name}}Client) tryConnect() (err error) {
	if c.rpcClient != nil {
		return
	}

	rpcClient, err := rpc.DialHTTP("tcp", c.host)
	if err != nil {
		return
	}
	c.rpcClient = rpcClient
	return
}

func (c *{{.Name}}Client) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	if err == rpc.ErrShutdown {
		c.rpcClient = nil
	}
	return
}
`

const funcTemplate = `
func (c *{{.TypeName}}Client) {{.FunctionName}}({{.FieldsAsParams}}) ({{if not .NoReturnValue}}{{.ReturnName}} *{{.ReturnType}}, {{end}}err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var {{.ReplyName}} {{.ReplyType}}
	err = c.call("{{.TypeName}}.{{.FunctionName}}", &{{.ParamType}}{ {{.FieldsAsArgs}} }, &{{.ReplyName}})
	if err != nil {
		log.Println("{{.TypeName}}Client.{{.FunctionName}}:", err)
		return
	}
	{{if .FlattenedReturn}}
	{{.ReturnName}} = &{{.ReplyName}}.{{.ReturnName}}
	{{end}}
	return
}
`

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("Usage: %f fileWithServerClass.go", os.Args[0])
		return
	}

	GenerateRpcClient(os.Args[1])
}

func GenerateRpcClient(serverFile string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, serverFile, nil, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	type RpcFuncDecl struct {
		TypeName        string
		FunctionName    string
		ParamType       string
		FieldsAsParams  string
		FieldsAsArgs    string
		ReplyName       string
		ReplyType       string
		ReturnName      string
		ReturnType      string
		FlattenedReturn bool
		NoReturnValue   bool
	}

	type Field struct {
		Name     string
		TypeName string
	}

	type Type struct {
		Name            string
		Fields          []Field
		Funcs           []RpcFuncDecl
		FieldsAsParams  string
		FieldsAsArgs    string
		ReturnName      string
		ReturnType      string
		FlattenedReturn bool
	}

	types := make(map[string]*Type)

	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.GenDecl:
			if d.Tok == token.TYPE {
				for _, s := range d.Specs {
					ts, ok := s.(*ast.TypeSpec)
					if ok {
						st, ok := ts.Type.(*ast.StructType)
						if ok {
							t := &Type{ts.Name.Name, make([]Field, len(st.Fields.List)), make([]RpcFuncDecl, 0), "", "", "reply", fmt.Sprintf("*%v", ts.Name.Name), false}
							types[ts.Name.Name] = t
							for i, field := range st.Fields.List {
								switch fieldType := field.Type.(type) {
								case *ast.ArrayType:
									fieldArrayType, ok := fieldType.Elt.(*ast.Ident)
									if ok {
										t.Fields[i] = Field{field.Names[0].Name, "[]" + fieldArrayType.Name}
									}
								case *ast.Ident:
									t.Fields[i] = Field{field.Names[0].Name, fieldType.Name}
								}
							}

							var params = []string{}
							for _, field := range t.Fields {
								if len(field.Name) > 0 {
									params = append(params, fmt.Sprintf("%v %v", strings.ToLower(field.Name), field.TypeName))
								}
							}
							t.FieldsAsParams = strings.Join(params, ", ")

							var args = []string{}
							for _, field := range t.Fields {
								if len(field.Name) > 0 {
									args = append(args, strings.ToLower(field.Name))
								}
							}
							t.FieldsAsArgs = strings.Join(args, ", ")

							if len(t.Fields) == 1 {
								t.FlattenedReturn = true
								t.ReturnName = t.Fields[0].Name
								t.ReturnType = t.Fields[0].TypeName
							}
						}
					}
				}
			}
		case *ast.FuncDecl:
			if d.Name.IsExported() && len(d.Type.Params.List) == 2 && d.Recv != nil {
				recv, ok := d.Recv.List[0].Type.(*ast.StarExpr)
				if ok {
					class := recv.X.(*ast.Ident)
					if class.IsExported() {
						paramNames := make([]string, 2)
						paramTypes := make([]string, 2)
						for i := 0; i < 2; i++ {
							paramNames[i] = d.Type.Params.List[i].Names[0].Name
							param, ok := d.Type.Params.List[i].Type.(*ast.StarExpr)
							if ok {
								paramTypes[i] = param.X.(*ast.Ident).Name
							} else {
								break
							}
						}
						if ok {
							t := types[class.Name]
							paramType := types[paramTypes[0]]
							returnName := paramNames[1]
							returnType := paramTypes[1]
							flattenedReturn := false
							rt, ok := types[paramTypes[1]]
							if ok {
								returnName = rt.ReturnName
								returnType = rt.ReturnType
								flattenedReturn = rt.FlattenedReturn
							}
							noReturnValue := false
							if paramNames[1] == "_" {
								paramNames[1] = "reply"
								noReturnValue = true
							}

							t.Funcs = append(t.Funcs, RpcFuncDecl{
								class.Name,
								d.Name.Name,
								paramTypes[0],
								paramType.FieldsAsParams,
								paramType.FieldsAsArgs,
								paramNames[1],
								paramTypes[1],
								returnName,
								returnType,
								flattenedReturn,
								noReturnValue})
						}
					}
				}
			}
		}
	}

	for key, _ := range types {
		t := types[key]
		if len(t.Funcs) > 0 {
			printTemplate("class", classTemplate, t)
		}
		for _, data := range types[key].Funcs {

			printTemplate("func", funcTemplate, data)
		}
	}
}

func printTemplate(name string, templateString string, data interface{}) {
	tmpl, err := template.New(name).Parse(templateString)

	if err != nil {
		panic(err)
	}
	err = tmpl.Execute(os.Stdout, data)
	if err != nil {
		panic(err)
	}
}

// The following exists so that generateRpcClient.go can be run with itself as an argument

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}
