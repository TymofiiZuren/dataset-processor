package awstemplate

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

func template(t *testing.T) map[string]any {
	t.Helper()
	b, e := os.ReadFile("template.json")
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	if e = json.Unmarshal(b, &v); e != nil {
		t.Fatal(e)
	}
	return v
}
func TestFoundationRetainsDataAndScopesRoleCapabilities(t *testing.T) {
	resources := template(t)["Resources"].(map[string]any)
	bucket := resources["DataBucket"].(map[string]any)
	props := bucket["Properties"].(map[string]any)
	if bucket["DeletionPolicy"] != "Retain" || bucket["UpdateReplacePolicy"] != "Retain" || props["VersioningConfiguration"].(map[string]any)["Status"] != "Enabled" {
		t.Fatal("selected object versions not retained")
	}
	if _, present := props["LifecycleConfiguration"]; present {
		t.Fatal("uncoordinated lifecycle deletion")
	}
	for _, v := range props["PublicAccessBlockConfiguration"].(map[string]any) {
		if v != true {
			t.Fatal("public access block missing")
		}
	}
	forbidden := map[string]map[string]bool{"ApiPolicy": {"sqs:ReceiveMessage": true, "sqs:SendMessage": true}, "WorkerPolicy": {"sqs:SendMessage": true}, "CoordinatorPolicy": {"sqs:ReceiveMessage": true}}
	required := map[string][]string{"ApiPolicy": {"s3:GetObjectVersion", "s3:PutObject"}, "WorkerPolicy": {"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "s3:GetObjectVersion", "s3:PutObject"}, "CoordinatorPolicy": {"sqs:SendMessage", "s3:GetObject", "s3:PutObject"}}
	for name, banned := range forbidden {
		statements := resources[name].(map[string]any)["Properties"].(map[string]any)["PolicyDocument"].(map[string]any)["Statement"].([]any)
		seen := map[string]bool{}
		for _, value := range statements {
			statement := value.(map[string]any)
			if statement["Effect"] != "Allow" {
				continue
			}
			if statement["Resource"] == "*" {
				t.Fatal("wildcard resource privilege")
			}
			for _, a := range statement["Action"].([]any) {
				action := a.(string)
				if strings.Contains(action, "*") || strings.Contains(action, "DeleteObject") || banned[action] {
					t.Fatalf("role %s has excessive capability %s", name, action)
				}
				seen[action] = true
			}
		}
		for _, action := range required[name] {
			if !seen[action] {
				t.Fatalf("role %s cannot perform required %s", name, action)
			}
		}
	}
	if resources["WorkQueue"].(map[string]any)["Properties"].(map[string]any)["SqsManagedSseEnabled"] != true || resources["DeadLetterQueue"].(map[string]any)["Properties"].(map[string]any)["SqsManagedSseEnabled"] != true {
		t.Fatal("queue encryption missing")
	}
}
func TestFoundationHasNoDependencyCycleOrEmbeddedCredentials(t *testing.T) {
	doc := template(t)
	resources := doc["Resources"].(map[string]any)
	params := doc["Parameters"].(map[string]any)
	graph := map[string]map[string]bool{}
	for name, value := range resources {
		graph[name] = map[string]bool{}
		var walk func(any)
		ref := func(target string) {
			target = strings.Split(target, ".")[0]
			if _, ok := resources[target]; ok {
				graph[name][target] = true
			} else if _, ok := params[target]; !ok && !strings.HasPrefix(target, "AWS::") {
				t.Fatalf("unknown resource reference %s", target)
			}
		}
		walk = func(v any) {
			switch x := v.(type) {
			case map[string]any:
				for k, item := range x {
					switch k {
					case "Ref":
						ref(item.(string))
					case "Fn::GetAtt":
						ref(item.([]any)[0].(string))
					case "Fn::Sub":
						for _, match := range regexp.MustCompile(`\$\{([^}]+)\}`).FindAllStringSubmatch(item.(string), -1) {
							ref(match[1])
						}
					default:
						walk(item)
					}
				}
			case []any:
				for _, item := range x {
					walk(item)
				}
			}
		}
		walk(value.(map[string]any)["Properties"])
	}
	done, active := map[string]bool{}, map[string]bool{}
	var visit func(string)
	visit = func(n string) {
		if active[n] {
			t.Fatal("CloudFormation dependency cycle")
		}
		if done[n] {
			return
		}
		active[n] = true
		for next := range graph[n] {
			visit(next)
		}
		active[n] = false
		done[n] = true
	}
	for n := range graph {
		visit(n)
	}
	b, _ := json.Marshal(doc)
	for _, forbidden := range []string{"AWS::IAM::AccessKey", "AWS::RDS::DBInstance", "AWS::CodePipeline::Pipeline", "AWS::IAM::User"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatal("foundation expanded into credentials, database or hosted pipeline")
		}
	}
}
