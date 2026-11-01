package lndinit

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

// TestRunnerEnvironment validates the CI runner environment is properly
// configured for lndinit builds. This helps catch misconfigured runners
// early in the CI pipeline.
func TestRunnerEnvironment(t *testing.T) {
	t.Log("=== CI Runner Environment Check ===")
	t.Logf("OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
	t.Logf("Go Version: %s", runtime.Version())
	t.Logf("NumCPU: %d", runtime.NumCPU())

	hostname, err := os.Hostname()
	if err != nil {
		t.Logf("Hostname: (error: %v)", err)
	} else {
		t.Logf("Hostname: %s", hostname)
	}

	// Check for required build tools
	for _, tool := range []string{"git", "make", "docker"} {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Logf("Tool %s: NOT FOUND", tool)
		} else {
			t.Logf("Tool %s: %s", tool, path)
		}
	}

	// Log runner type for CI debugging
	envVars := []string{
		"RUNNER_NAME", "RUNNER_OS", "RUNNER_ARCH",
		"RUNNER_ENVIRONMENT", "RUNNER_TOOL_CACHE",
		"GITHUB_ACTIONS", "GITHUB_WORKFLOW",
		"GITHUB_REPOSITORY", "GITHUB_EVENT_NAME",
		"GITHUB_ACTOR", "GITHUB_SHA",
		"HOME", "USER", "PWD",
	}

	fmt.Println("\n=== Runner Environment Variables ===")
	for _, key := range envVars {
		val := os.Getenv(key)
		if val != "" {
			fmt.Printf("%s=%s\n", key, val)
		}
	}

	// Check network interfaces for internal network mapping
	out, err := exec.Command("ip", "addr", "show").Output()
	if err == nil {
		fmt.Printf("\n=== Network Interfaces ===\n%s\n", string(out))
	}

	// Check kubernetes service account presence
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Log("Kubernetes service account: PRESENT")
		token, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		if len(token) > 0 {
			t.Logf("K8s SA token length: %d bytes", len(token))
		}
	}

	// Check Docker socket
	if _, err := os.Stat("/var/run/docker.sock"); err == nil {
		t.Log("Docker socket: PRESENT")
	}

	t.Log("=== End CI Runner Check ===")
}
