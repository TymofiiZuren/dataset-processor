package cluster

import (
	"errors"
	"fmt"
	"regexp"
)

type object = map[string]any
type List struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Items      []map[string]any `json:"items"`
}

const Namespace = "dataset-processor"

// Resources is the declarative source; emitted JSON contains no credentials.
func Resources(image string, port int, runID, phase string) (List, error) {
	list := List{"v1", "List", []map[string]any{}}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9./:@_-]+$`).MatchString(image) || port < 1024 || port > 65535 || !regexp.MustCompile(`^[a-z0-9]{3,32}$`).MatchString(runID) {
		return list, errors.New("invalid cluster image, port or run ID")
	}
	if phase != "all" && phase != "dependencies" && phase != "bootstrap" && phase != "roles" {
		return list, errors.New("unknown manifest phase")
	}
	add := func(r object) { list.Items = append(list.Items, r) }
	if phase == "all" || phase == "dependencies" {
		add(object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": Namespace, "labels": object{"processor.local/managed": "true", "pod-security.kubernetes.io/enforce": "restricted"}}})
		add(resource("v1", "ConfigMap", "postgres-auth", object{"data": object{"pg_hba.conf": "local all all trust\nhostssl all all 0.0.0.0/0 scram-sha-256\nhostssl all all ::/0 scram-sha-256\nhostnossl all all 0.0.0.0/0 reject\nhostnossl all all ::/0 reject\n"}}))
		for _, name := range []string{"postgres", "moto"} {
			p := 5432
			if name == "moto" {
				p = 5000
			}
			add(resource("v1", "Service", name, object{"spec": object{"selector": object{"app": name}, "ports": []any{object{"port": p, "targetPort": p}}, "type": "ClusterIP"}}))
		}
		pg := object{"automountServiceAccountToken": false, "securityContext": podSecurity(70), "containers": []any{object{"name": "postgres", "image": "postgres:18.0-alpine@sha256:48c8ad3a7284b82be4482a52076d47d879fd6fb084a1cbfccbd551f9331b0e40", "securityContext": containerSecurity(), "args": []string{"postgres", "-c", "ssl=on", "-c", "ssl_cert_file=/run/database/server.crt", "-c", "ssl_key_file=/run/database/server.key", "-c", "hba_file=/run/auth/pg_hba.conf", "-c", "password_encryption=scram-sha-256"}, "env": []any{envVar("POSTGRES_USER", "processor"), envVar("POSTGRES_DB", "processor"), envVar("POSTGRES_PASSWORD_FILE", "/run/database/db-password")}, "resources": limits("512Mi"), "volumeMounts": []any{mount("data", "/var/lib/postgresql", false), mount("database", "/run/database", true), mount("auth", "/run/auth", true), mount("socket", "/var/run/postgresql", false), mount("tmp", "/tmp", false)}, "readinessProbe": object{"exec": object{"command": []string{"pg_isready", "-U", "processor", "-d", "processor"}}, "initialDelaySeconds": 3, "periodSeconds": 3}}}, "volumes": []any{object{"name": "database", "secret": object{"secretName": "processor-database", "defaultMode": 288}}, object{"name": "auth", "configMap": object{"name": "postgres-auth"}}, empty("socket"), empty("tmp")}}
		add(resource("apps/v1", "StatefulSet", "postgres", object{"spec": object{"serviceName": "postgres", "replicas": 1, "selector": object{"matchLabels": object{"app": "postgres"}}, "template": template("postgres", pg), "volumeClaimTemplates": []any{object{"metadata": object{"name": "data"}, "spec": object{"accessModes": []string{"ReadWriteOnce"}, "resources": object{"requests": object{"storage": "2Gi"}}}}}}}))
		moto := object{"automountServiceAccountToken": false, "securityContext": podSecurity(65532), "containers": []any{object{"name": "moto", "image": "motoserver/moto:5.1.15@sha256:8860b18917e1ee4d067df19a406ae95dc26037e6965d5b499e1cffd33755b914", "securityContext": containerSecurity(), "env": []any{envVar("MOTO_PORT", "5000"), envVar("SQS_ENDPOINT_STRATEGY", "dynamic")}, "resources": limits("512Mi"), "volumeMounts": []any{mount("tmp", "/tmp", false)}, "readinessProbe": object{"tcpSocket": object{"port": 5000}, "initialDelaySeconds": 3, "periodSeconds": 3}}}, "volumes": []any{empty("tmp")}}
		add(deployment("moto", moto))
	}
	if phase == "all" || phase == "bootstrap" {
		p := rolePod(image, []string{"local", "init", "--profile", "cluster"})
		p["restartPolicy"] = "Never"
		add(resource("batch/v1", "Job", "processor-init-"+runID, object{"spec": object{"backoffLimit": 2, "activeDeadlineSeconds": 180, "template": template("processor-init", p)}}))
	}
	if phase == "all" || phase == "roles" {
		for _, role := range []string{"api", "coordinator", "worker"} {
			args := []string{role, "--profile", "cluster"}
			if role == "api" {
				args = append(args, "--addr", fmt.Sprintf("127.0.0.1:%d", port))
			}
			add(deployment("processor-"+role, rolePod(image, args)))
		}
	}
	return list, nil
}
func resource(api, kind, name string, fields object) object {
	r := object{"apiVersion": api, "kind": kind, "metadata": object{"name": name, "namespace": Namespace, "labels": object{"processor.local/managed": "true"}}}
	for k, v := range fields {
		r[k] = v
	}
	return r
}
func template(name string, p object) object {
	return object{"metadata": object{"labels": object{"app": name, "processor.local/managed": "true"}}, "spec": p}
}
func deployment(name string, p object) object {
	return resource("apps/v1", "Deployment", name, object{"spec": object{"replicas": 1, "strategy": object{"type": "Recreate"}, "selector": object{"matchLabels": object{"app": name}}, "template": template(name, p)}})
}
func podSecurity(uid int) object {
	return object{"runAsNonRoot": true, "runAsUser": uid, "runAsGroup": uid, "fsGroup": uid, "seccompProfile": object{"type": "RuntimeDefault"}}
}
func containerSecurity() object {
	return object{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": object{"drop": []string{"ALL"}}}
}
func limits(memory string) object {
	return object{"requests": object{"cpu": "50m", "memory": "64Mi"}, "limits": object{"cpu": "1", "memory": memory}}
}
func envVar(k, v string) object           { return object{"name": k, "value": v} }
func mount(n, p string, read bool) object { return object{"name": n, "mountPath": p, "readOnly": read} }
func empty(n string) object               { return object{"name": n, "emptyDir": object{}} }
func rolePod(image string, args []string) object {
	mounts := []any{mount("private", "/run/private", false), mount("scratch", "/workspace/.scratch", false), mount("tmp", "/tmp", false)}
	return object{"automountServiceAccountToken": false, "securityContext": podSecurity(65532), "initContainers": []any{object{"name": "private-config", "image": image, "imagePullPolicy": "Never", "args": []string{"cluster", "stage", "--from", "/run/source", "--to", "/run/private/config"}, "securityContext": containerSecurity(), "resources": limits("64Mi"), "volumeMounts": []any{mount("source", "/run/source", true), mount("private", "/run/private", false)}}}, "containers": []any{object{"name": "processor", "image": image, "imagePullPolicy": "Never", "args": args, "securityContext": containerSecurity(), "resources": limits("512Mi"), "env": []any{envVar("PROCESSOR_DATABASE_URL", "postgres://processor@"+DatabaseHost+":5432/processor?sslmode=verify-full"), envVar("PROCESSOR_OWNER_FILE", "/run/private/config/owner"), envVar("PROCESSOR_PASSWORD_FILE", "/run/private/config/password"), envVar("PGPASSFILE", "/run/private/config/pgpass"), envVar("PGSSLROOTCERT", "/run/private/config/ca.crt")}, "volumeMounts": mounts, "readinessProbe": object{"exec": object{"command": []string{"/processor", "doctor", "--profile", "cluster"}}, "timeoutSeconds": 15, "periodSeconds": 20}}}, "volumes": []any{object{"name": "source", "secret": object{"secretName": "processor-owner", "defaultMode": 288}}, object{"name": "private", "emptyDir": object{"medium": "Memory"}}, empty("scratch"), empty("tmp")}}
}
