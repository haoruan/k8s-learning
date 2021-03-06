package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/emicklei/go-restful"
)

type Instance struct {
	genericAPIServer *GenericAPIServer
}

type consumeType struct {
	Name string `json:"name"`
}

type produceType struct {
	Path string      `json:"path"`
	Body consumeType `json:"body"`
}

func newWebService(prefix string) *restful.WebService {
	ws := new(restful.WebService)
	ws.Path(prefix).
		Doc("API at" + prefix).
		Consumes("*/*").
		Produces("*/*").
		ApiVersion("v1")

	return ws
}

func installLegacyAPI() *restful.WebService {
	ws := newWebService("/api")
	routes := []*restful.RouteBuilder{}
	verbs := []string{"CREATE", "GET", "UPDATE", "DELETE"}

	for _, v := range verbs {
		switch v {
		case "CREATE":
			doc := "create sample resources"
			route := ws.POST("/create").
				To(handler).
				Doc(doc).
				Param(ws.QueryParameter("test", "test query param")).
				Produces(restful.MIME_JSON).
				Returns(http.StatusOK, "OK", nil).
				Reads(consumeType{}).
				Writes(produceType{})

			routes = append(routes, route)
		case "GET":
			doc := "get sample resources"
			route := ws.GET("/get").
				To(handler).
				Doc(doc).
				Param(ws.QueryParameter("test", "test query param")).
				Produces(restful.MIME_JSON).
				Returns(http.StatusOK, "OK", nil).
				Reads(consumeType{}).
				Writes(produceType{})

			routes = append(routes, route)
		case "UPDATE":
			doc := "update sample resources"
			route := ws.PATCH("/update").
				To(handler).
				Doc(doc).
				Param(ws.QueryParameter("test", "test query param")).
				Produces(restful.MIME_JSON).
				Returns(http.StatusOK, "OK", nil).
				Reads(consumeType{}).
				Writes(produceType{})

			routes = append(routes, route)
		case "DELETE":
			doc := "delete sample resources"
			route := ws.DELETE("/delete").
				To(handler).
				Doc(doc).
				Param(ws.QueryParameter("test", "test query param")).
				Produces(restful.MIME_JSON).
				Returns(http.StatusOK, "OK", nil).
				Reads(consumeType{}).
				Writes(produceType{})

			routes = append(routes, route)
		}
	}

	for _, route := range routes {
		ws.Route(route)
	}

	return ws
}

func handler(req *restful.Request, res *restful.Response) {
	defer req.Request.Body.Close()
	url := req.Request.URL
	body, err := ioutil.ReadAll(req.Request.Body)

	var oriBody consumeType
	json.Unmarshal(body, &oriBody)

	obj := produceType{url.Path, oriBody}

	if err != nil {
		WriteRawJSON(http.StatusInternalServerError, nil, res.ResponseWriter)
		return
	}

	WriteRawJSON(http.StatusOK, obj, res.ResponseWriter)
}

func (c completedConfig) NewAPIServer() (*Instance, error) {
	s, err := c.NewServer("sample-apiserver")
	if err != nil {
		return nil, err
	}

	ws := installLegacyAPI()

	s.Handler.GoRestfulContainer.Add(ws)

	return &Instance{s}, nil
}
