package main

import (
	"net/http"

	restful "github.com/emicklei/go-restful"
)

// Version provides a webservice with version information.
type Version struct {
	Major string `json:"major"`
	Minor string `json:"minor"`
}

// Install registers the APIServer's `/version` handler.
func (v Version) Install(c *restful.Container) {
	// Set up a service to return the git code version.
	versionWS := new(restful.WebService)
	versionWS.Path("/version")
	versionWS.Doc("git code version from which this is built")
	versionWS.Route(
		versionWS.GET("/").To(v.handleVersion).
			Doc("get the code version").
			Operation("getCodeVersion").
			Produces(restful.MIME_JSON).
			Consumes(restful.MIME_JSON).
			Writes(Version{}))

	c.Add(versionWS)
}

// handleVersion writes the server's version information.
func (v Version) handleVersion(req *restful.Request, resp *restful.Response) {
	WriteRawJSON(http.StatusOK, v, resp.ResponseWriter)
}
