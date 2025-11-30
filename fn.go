package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"text/template"

	"google.golang.org/protobuf/encoding/protojson"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/crossplane/function-sdk-go/errors"
	"github.com/crossplane/function-sdk-go/logging"
	fnv1 "github.com/crossplane/function-sdk-go/proto/v1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/response"

	"github.com/crossplane-contrib/function-go-templating/input/v1beta1"
)

// osFS is a dead-simple implementation of [io/fs.FS] that just wraps around
// [os.Open].
type osFS struct{}

func (*osFS) Open(name string) (fs.File, error) {
	return os.Open(name)
}

// Function uses Go templates to compose resources.
type Function struct {
	fnv1.UnimplementedFunctionRunnerServiceServer

	log  logging.Logger
	fsys fs.FS
}

type YamlErrorContext struct {
	RelLine int
	AbsLine int
	Message string
	Context string
}

const (
	annotationKeyCompositionResourceName = "gotemplating.fn.crossplane.io/composition-resource-name"
	annotationKeyReady                   = "gotemplating.fn.crossplane.io/ready"

	metaApiVersion = "meta.gotemplating.fn.crossplane.io/v1alpha1"
)

// RunFunction runs the Function.
func (f *Function) RunFunction(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	f.log.Debug("Running Function", "tag", req.GetMeta().GetTag())

	rsp := response.To(req, response.DefaultTTL)

	in := &v1beta1.GoTemplate{}
	if err := request.GetInput(req, in); err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot get Function input from %T", req))
		return rsp, nil
	}

	tg, err := NewTemplateSourceGetter(f.fsys, req.GetContext(), in)
	if err != nil {
		response.Fatal(rsp, errors.Wrap(err, "invalid function input"))
		return rsp, nil
	}

	f.log.Debug("template", "template", tg.GetTemplates())

	tmpl, err := GetNewTemplateWithFunctionMaps(in.Delims).Parse(tg.GetTemplates())
	if err != nil {
		response.Fatal(rsp, errors.Wrap(err, "invalid function input: cannot parse the provided templates"))
		return rsp, nil
	}

	if in.Options != nil {
		f.log.Debug("setting template options", "options", *in.Options)
		err = safeApplyTemplateOptions(tmpl, *in.Options)
		if err != nil {
			response.Fatal(rsp, errors.Wrap(err, "cannot apply template options"))
			return rsp, nil
		}
	}

	reqMap, err := convertToMap(req)
	if err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot convert request to map"))
		return rsp, nil
	}

	f.log.Debug("constructed request map", "request", reqMap)

	buf := &bytes.Buffer{}

	if err := tmpl.Execute(buf, reqMap); err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot execute template"))
		return rsp, nil
	}

	f.log.Debug("rendered manifests", "manifests", buf.String())

	// Parse the rendered manifests.
	data := buf.String()
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(data), 1024)

	if err := decoder.Decode(&rsp); err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot decode manifest"))
		return rsp, nil
	}

	f.log.Debug("Successfully composed desired resources", "source", in.Source, "count", len(rsp.Desired.Resources))

	return rsp, nil
}

func convertToMap(req *fnv1.RunFunctionRequest) (map[string]any, error) {
	jReq, err := protojson.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal request from proto to json")
	}

	var mReq map[string]any
	if err := json.Unmarshal(jReq, &mReq); err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal json to map[string]any")
	}

	_, ok := mReq["extraResources"]
	if !ok {
		r, ok := mReq["requiredResources"]
		if ok {
			mReq["extraResources"] = r
		}
	}

	return mReq, nil
}

func safeApplyTemplateOptions(templ *template.Template, options []string) (err error) {
	defer func() {
		rec := recover()
		if rec != nil {
			err = errors.Errorf("panic occurred while applying template options: %v", rec)
		}
	}()
	templ.Option(options...)
	return nil
}

func moveToNextDoc(lines []string, startLine int) int {
	for i := startLine; i <= len(lines); i++ {
		if strings.TrimSpace(lines[i-1]) == "---" && i > startLine {
			return i
		}
	}
	return startLine
}

func getYamlErrorContextFromErr(err error, startLine int, lines []string) YamlErrorContext {
	var relLine int
	n, scanErr := fmt.Sscanf(err.Error(), "error converting YAML to JSON: yaml: line %d:", &relLine)
	var errMsg string
	if scanErr == nil && n == 1 {
		// Extract the rest of the error message after the matched prefix.
		prefix := fmt.Sprintf("error converting YAML to JSON: yaml: line %d:", relLine)
		errStr := err.Error()
		if idx := strings.Index(errStr, prefix); idx != -1 {
			errMsg = strings.TrimSpace(errStr[idx+len(prefix):])
		}
	}
	if scanErr == nil && n == 1 {
		absLine := startLine + relLine
		if absLine-1 < len(lines) && absLine-1 >= 0 {
			return YamlErrorContext{
				RelLine: relLine,
				AbsLine: absLine,
				Message: errMsg,
				Context: lines[absLine-1],
			}
		}
	}
	return YamlErrorContext{}
}
