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
	defaultK8sNamespace      = "default"
	defaultK8sResourcePolicy = "keep"
)

type k8sObjectType string

const (
	ObjectTypeSecret    k8sObjectType = "Secret"
	ObjectTypeConfigMap k8sObjectType = "ConfigMap"
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

type k8sConfigmapOptions struct {
	Namespace string `long:"namespace" description:"The Kubernetes namespace the configmap is located in"`
	Name      string `long:"configmap-name" description:"The name of the Kubernetes configmap"`
	KeyName   string `long:"configmap-key-name" description:"The name of the key/entry within the configmap"`
}

type k8sObjectOptions struct {
	Namespace  string
	Name       string
	KeyName    string
	Base64     bool
	ObjectType k8sObjectType
}

type helmOptions struct {
	Annotate       bool   `long:"annotate" description:"Whether Helm annotations should be added to the created secret"`
	ReleaseName    string `long:"release-name" description:"The value for the meta.helm.sh/release-name annotation"`
	ResourcePolicy string `long:"resource-policy" description:"The value for the helm.sh/resource-policy annotation"`
}

type jsonK8sObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

func saveK8s(content string, opts *k8sObjectOptions,
	overwrite bool, helm *helmOptions) error {

	client, err := getClientK8s()
	if err != nil {
		return err
	}

	switch opts.ObjectType {
	case ObjectTypeSecret:
		return saveSecretK8s(client, content, opts, overwrite, helm)
	case ObjectTypeConfigMap:
		return saveConfigMapK8s(client, content, opts, overwrite, helm)
	default:
		return fmt.Errorf("unsupported object type: %s", opts.ObjectType)
	}
}

func saveSecretK8s(client *kubernetes.Clientset, content string,
	opts *k8sObjectOptions, overwrite bool, helm *helmOptions) error {

	secret, exists, err := getSecretK8s(client, opts.Namespace, opts.Name)
	if err != nil {
		return err
	}

	if exists {
		return updateSecretValueK8s(
			client, secret, opts, overwrite, content,
		)
	}

	return createSecretK8s(client, opts, helm, content)
}

func saveConfigMapK8s(client *kubernetes.Clientset, content string,
	opts *k8sObjectOptions, overwrite bool, helm *helmOptions) error {

	configMap, exists, err := getConfigMapK8s(
		client, opts.Namespace, opts.Name,
	)
	if err != nil {
		return err
	}

	if exists {
		return updateConfigMapValueK8s(
			client, configMap, opts, overwrite, content,
		)
	}

	return createConfigMapK8s(client, opts, helm, content)
}

func readK8s(opts *k8sObjectOptions) (string, *jsonK8sObject, error) {
	client, err := getClientK8s()
	if err != nil {
		return "", nil, err
	}

	switch opts.ObjectType {
	case ObjectTypeSecret:
		return readSecretK8s(client, opts)
	default:
		return "", nil, fmt.Errorf("unsupported object type: %s",
			opts.ObjectType)
	}
}

func readSecretK8s(client *kubernetes.Clientset,
	opts *k8sObjectOptions) (string, *jsonK8sObject, error) {

	// Existing logic to read a secret
	secret, exists, err := getSecretK8s(
		client, opts.Namespace, opts.Name,
	)
	if err != nil {
		return "", nil, err
	}

	if !exists {
		return "", nil, fmt.Errorf("secret %s does not exist in "+
			"namespace %s", opts.Name, opts.Namespace)
	}

	if len(secret.Data) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but contains no "+
			"data", opts.Name)
	}

	if len(secret.Data[opts.KeyName]) == 0 {
		return "", nil, fmt.Errorf("secret %s exists but does not "+
			"contain the key %s", opts.Name,
			opts.KeyName)
	}

	// There is an additional layer of base64 encoding applied to each of
	// the secrets. Try to de-code it now.
	content, err := secretToString(
		secret.Data[opts.KeyName], opts.Base64,
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode raw secret %s "+
			"key %s: %v", opts.Name, opts.KeyName, err)
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

func updateSecretValueK8s(client *kubernetes.Clientset, secret *api.Secret,
	opts *k8sObjectOptions, overwrite bool, content string) error {

	if len(secret.Data) == 0 {
		log("Data of secret %s is empty, initializing", opts.Name)
		secret.Data = make(map[string][]byte)
	}

	if len(secret.Data[opts.KeyName]) > 0 && !overwrite {
		return fmt.Errorf("key %s in secret %s already exists: %v",
			opts.KeyName, opts.Name,
			errTargetExists)
	}

	// Do we need to add an extra layer of base64?
	if opts.Base64 {
		content = base64.StdEncoding.EncodeToString([]byte(content))
	}
	secret.Data[opts.KeyName] = []byte(content)

	log("Attempting to update key %s of secret %s in namespace %s",
		opts.KeyName, opts.Name, opts.Namespace)
	updatedSecret, err := client.CoreV1().Secrets(opts.Namespace).Update(
		context.Background(), secret, metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("error updating secret %s in namespace %s: "+
			"%v", opts.Name, opts.Namespace, err)
	}

	jsonSecret, _ := asJSON(jsonK8sObject{
		TypeMeta:   updatedSecret.TypeMeta,
		ObjectMeta: updatedSecret.ObjectMeta,
	})
	log("Updated secret: %s", jsonSecret)

	return nil
}

func createSecretK8s(client *kubernetes.Clientset, opts *k8sObjectOptions,
	helm *helmOptions, content string) error {

	meta := metav1.ObjectMeta{
		Name: opts.Name,
	}

	if helm != nil && helm.Annotate {
		meta.Labels = map[string]string{
			"app.kubernetes.io/managed-by": "Helm",
		}
		meta.Annotations = map[string]string{
			"helm.sh/resource-policy":        helm.ResourcePolicy,
			"meta.helm.sh/release-name":      helm.ReleaseName,
			"meta.helm.sh/release-namespace": opts.Namespace,
		}
	}

	// Do we need to add an extra layer of base64?
	if opts.Base64 {
		content = base64.StdEncoding.EncodeToString([]byte(content))
	}

	newSecret := &api.Secret{
		Type:       api.SecretTypeOpaque,
		ObjectMeta: meta,
		Data: map[string][]byte{
			opts.KeyName: []byte(content),
		},
	}

	updatedSecret, err := client.CoreV1().Secrets(opts.Namespace).Create(
		context.Background(), newSecret, metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("error creating secret %s in namespace %s: "+
			"%v", opts.Name, opts.Namespace, err)
	}

	jsonSecret, _ := asJSON(jsonK8sObject{
		TypeMeta:   updatedSecret.TypeMeta,
		ObjectMeta: updatedSecret.ObjectMeta,
	})
	log("Created secret: %s", jsonSecret)

	return nil
}

// secretToString turns the raw bytes of a secret into a string, removing the
// additional layer of base64 encoding if there is expected to be one.
func secretToString(rawSecret []byte, doubleBase64 bool) (string, error) {
	content := string(rawSecret)
	if doubleBase64 {
		decoded, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			return "", fmt.Errorf("failed to base64 decode: %v",
				err)
		}

		content = string(decoded)
	}

	return content, nil
}

func getConfigMapK8s(client *kubernetes.Clientset,
	namespace, name string) (*api.ConfigMap, bool, error) {

	log("Attempting to load configmap %s from namespace %s", name, namespace)
	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(
		context.Background(), name, metav1.GetOptions{},
	)

	switch {
	case err == nil:
		log("ConfigMap %s loaded successfully", name)
		return configMap, true, nil

	case errors.IsNotFound(err):
		log("ConfigMap %s not found in namespace %s", name, namespace)
		return nil, false, nil

	default:
		return nil, false, fmt.Errorf("error querying configmap "+
			"existence: %v", err)
	}
}

func updateConfigMapValueK8s(client *kubernetes.Clientset,
	configMap *api.ConfigMap, opts *k8sObjectOptions,
	overwrite bool, content string) error {

	if configMap.Data == nil {
		log("Data of configmap %s is empty, initializing", opts.Name)
		configMap.Data = make(map[string]string)
	}

	if _, exists := configMap.Data[opts.KeyName]; exists && !overwrite {
		return fmt.Errorf("key %s in configmap %s already exists",
			opts.KeyName, opts.Name)
	}

	log("Attempting to update key %s of configmap %s in namespace %s",
		opts.KeyName, opts.Name, opts.Namespace)

	configMap.Data[opts.KeyName] = content
	updatedConfigMap, err := client.CoreV1().ConfigMaps(opts.Namespace).Update(
		context.Background(), configMap, metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("error updating configmap %s in namespace %s: %v",
			opts.Name, opts.Namespace, err)
	}

	jsonConfigMap, _ := asJSON(jsonK8sObject{
		TypeMeta:   updatedConfigMap.TypeMeta,
		ObjectMeta: updatedConfigMap.ObjectMeta,
	})
	log("Updated configmap: %s", jsonConfigMap)

	return nil
}

func createConfigMapK8s(client *kubernetes.Clientset,
	opts *k8sObjectOptions, helm *helmOptions, content string) error {

	meta := metav1.ObjectMeta{
		Name: opts.Name,
	}

	if helm != nil && helm.Annotate {
		meta.Labels = map[string]string{
			"app.kubernetes.io/managed-by": "Helm",
		}
		meta.Annotations = map[string]string{
			"helm.sh/resource-policy":        helm.ResourcePolicy,
			"meta.helm.sh/release-name":      helm.ReleaseName,
			"meta.helm.sh/release-namespace": opts.Namespace,
		}
	}

	newConfigMap := &api.ConfigMap{
		ObjectMeta: meta,
		Data: map[string]string{
			opts.KeyName: content,
		},
	}

	updatedConfigMap, err := client.CoreV1().ConfigMaps(opts.Namespace).Create(
		context.Background(), newConfigMap, metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("error creating configmap %s in namespace %s: %v",
			opts.Name, opts.Namespace, err)
	}

	jsonConfigMap, _ := asJSON(jsonK8sObject{
		TypeMeta:   updatedConfigMap.TypeMeta,
		ObjectMeta: updatedConfigMap.ObjectMeta,
	})
	log("Created configmap: %s", jsonConfigMap)

	return nil
}
