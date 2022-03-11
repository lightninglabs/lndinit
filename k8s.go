package main

import (
	"context"
	"encoding/base64"
	"fmt"

	api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	defaultK8sNamespace = "default"
)

type k8sSecretOptions struct {
	Namespace     string `long:"namespace" description:"The Kubernetes namespace the secret is located in"`
	SecretName    string `long:"secret-name" description:"The name of the Kubernetes secret"`
	SecretKeyName string `long:"secret-key-name" description:"The name of the key/entry within the secret"`
	Base64        bool   `long:"base64" description:"Encode as base64 when storing and decode as base64 when reading"`
}

func (s *k8sSecretOptions) AnySet() bool {
	return s.Namespace != defaultK8sNamespace || s.SecretName != "" ||
		s.SecretKeyName != ""
}

type jsonK8sObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

func readK8s(opts *k8sSecretOptions) (string, *jsonK8sObject, error) {
	client, err := getClientK8s()
	if err != nil {
		return "", nil, err
	}

	secret, exists, err := getSecretK8s(
		client, opts.Namespace, opts.SecretName,
	)
	if err != nil {
		return "", nil, err
	}

	if !exists {
		return "", nil, fmt.Errorf("secret %s does not exist in "+
			"namespace %s", opts.SecretName, opts.Namespace)
	}

	if len(secret.Data) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but contains no "+
			"data", opts.SecretName)
	}

	if len(secret.Data[opts.SecretKeyName]) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but does not "+
			"contain the key %s", opts.SecretName,
			opts.SecretKeyName)
	}

	// Remove any newlines at the end of the file. We won't ever write a
	// newline ourselves but maybe the file was provisioned by another
	// process or user.
	content := stripNewline(string(secret.Data[opts.SecretKeyName]))

	// There is an additional layer of base64 encoding applied to each of
	// the secrets. Try to de-code it now.
	if opts.Base64 {
		decoded, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			return "", nil, fmt.Errorf("failed to base64 decode "+
				"secret %s key %s: %v", opts.SecretName,
				opts.SecretKeyName, err)
		}

		content = stripNewline(string(decoded))
	}

	return content, &jsonK8sObject{
		TypeMeta:   secret.TypeMeta,
		ObjectMeta: secret.ObjectMeta,
	}, nil
}

func getClientK8s() (*kubernetes.Clientset, error) {
	log("Creating k8s cluster config")
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to grab cluster config: %v", err)
	}

	log("Creating k8s cluster client")
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating cluster config: %v", err)
	}

	log("Cluster client created successfully")
	return client, nil
}

func getSecretK8s(client *kubernetes.Clientset, namespace,
	name string) (*api.Secret, bool, error) {

	log("Attempting to load secret %s from namespace %s", name, namespace)
	secret, err := client.CoreV1().Secrets(namespace).Get(
		context.Background(), name, metav1.GetOptions{},
	)

	switch {
	case err == nil:
		log("Secret %s loaded successfully", name)
		return secret, true, nil

	case errors.IsNotFound(err):
		log("Secret %s not found in namespace %s", name, namespace)
		return nil, false, nil

	default:
		return nil, false, fmt.Errorf("error querying secret "+
			"existence: %v", err)
	}
}
