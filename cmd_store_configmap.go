package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jessevdk/go-flags"
)

type targetK8sConfigmap struct {
	k8sConfigmapOptions

	Helm *helmOptions `group:"Flags for configuring the Helm annotations (use when --target=k8s)" namespace:"helm"`
}

type storeConfigmapCommand struct {
	Batch     bool                `long:"batch" description:"Instead of reading one configmap from stdin, read all files of the argument list and store them as entries in the configmap"`
	Overwrite bool                `long:"overwrite" description:"Overwrite existing configmap entries instead of aborting"`
	Target    string              `long:"target" short:"t" description:"Configmap storage target" choice:"k8s"`
	K8s       *targetK8sConfigmap `group:"Flags for storing the key/value pair inside a Kubernetes Configmap (use when --target=k8s)" namespace:"k8s"`
}

func newStoreConfigmapCommand() *storeConfigmapCommand {
	return &storeConfigmapCommand{
		Target: storageK8s,
		K8s: &targetK8sConfigmap{
			k8sConfigmapOptions: k8sConfigmapOptions{
				Namespace: defaultK8sNamespace,
			},
		},
	}
}

func (x *storeConfigmapCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"store-configmap",
		"Write key/value pairs to a Kubernetes configmap",
		"Read a configmap from stdin and store it to the "+
			"external configmaps storage indicated by the --target "+
			"flag; if the --batch flag is used, instead of "+
			"reading a single configmap entry from stdin, each command "+
			"line argument is treated as a file and each file's "+
			"content is added to the configmap with the file's name "+
			"as the key name for the configmap entry",
		x,
	)
	return err
}

func (x *storeConfigmapCommand) Execute(args []string) error {
	var entries []*entry

	switch {
	case x.Batch && len(args) == 0:
		return fmt.Errorf("at least one command line argument is " +
			"required when using --batch flag")

	case x.Batch:
		for _, file := range args {
			logger.Infof("Reading value/entry from file %s", file)
			content, err := readFile(file)
			if err != nil {
				return fmt.Errorf("cannot read file %s: %v",
					file, err)
			}

			entries = append(entries, &entry{
				key:   filepath.Base(file),
				value: content,
			})
		}

	default:
		logger.Info("Reading value/entry from stdin")
		value, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("error reading entry from stdin: %v", err)
		}
		entries = append(entries, &entry{value: string(value)})
	}

	switch x.Target {
	case storageK8s:
		// Take the actual entry key from the options if we aren't in
		// batch mode.
		if len(entries) == 1 && entries[0].key == "" {
			entries[0].key = x.K8s.KeyName
		}

		return storeConfigmapsK8s(entries, x.K8s, x.Overwrite)

	default:
		return fmt.Errorf("invalid configmap storage target %s", x.Target)
	}
}

func storeConfigmapsK8s(entries []*entry, opts *targetK8sConfigmap,
	overwrite bool) error {

	if opts.Name == "" {
		return fmt.Errorf("configmap name is required")
	}

	for _, entry := range entries {
		if entry.key == "" {
			return fmt.Errorf("configmap entry key name is required")
		}

		entryOpts := &k8sObjectOptions{
			Namespace:  opts.Namespace,
			Name:       opts.Name,
			KeyName:    entry.key,
			ObjectType: ObjectTypeConfigMap,
		}

		logger.Infof("Storing key with name %s to configmap %s in namespace %s",
			entryOpts.KeyName, entryOpts.Name,
			entryOpts.Namespace)

		err := saveK8s(entry.value, entryOpts, overwrite, opts.Helm)
		if err != nil {
			return fmt.Errorf("error storing key %s in configmap %s: "+
				"%v", entry.key, opts.Name, err)
		}
	}

	return nil
}
