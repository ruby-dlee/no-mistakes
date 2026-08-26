package config

import (
	"testing"
	"time"
)

func TestLoadGlobalAzureWorkerIsExplicitOptIn(t *testing.T) {
	defaults, err := LoadGlobalFromBytes([]byte("agent: auto\n"))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.AzureWorker.Enabled {
		t.Fatal("Azure worker transport enabled by default")
	}
	if defaults.AzureWorker.ReviewConcurrency != 1 || defaults.AzureWorker.RepairConcurrency != 1 || defaults.AzureWorker.TestConcurrency != 1 {
		t.Fatalf("default Azure worker concurrency = %+v", defaults.AzureWorker)
	}

	cfg, err := LoadGlobalFromBytes([]byte(`
agent: auto
azure_worker:
  enabled: true
  runner_path: /opt/firstmate/bin/fm-no-mistakes-worker
  config_path: /etc/firstmate/no-mistakes-worker.yaml
  lease_duration: 2m
  heartbeat_interval: 30s
  timeout: 20m
  review_concurrency: 3
  repair_concurrency: 2
  test_concurrency: 1
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AzureWorker.Enabled || cfg.AzureWorker.RunnerPath != "/opt/firstmate/bin/fm-no-mistakes-worker" || cfg.AzureWorker.ConfigPath != "/etc/firstmate/no-mistakes-worker.yaml" {
		t.Fatalf("Azure worker config = %+v", cfg.AzureWorker)
	}
	if cfg.AzureWorker.LeaseDuration != 2*time.Minute || cfg.AzureWorker.HeartbeatInterval != 30*time.Second || cfg.AzureWorker.Timeout != 20*time.Minute {
		t.Fatalf("Azure worker durations = %+v", cfg.AzureWorker)
	}
	if cfg.AzureWorker.ReviewConcurrency != 3 || cfg.AzureWorker.RepairConcurrency != 2 || cfg.AzureWorker.TestConcurrency != 1 {
		t.Fatalf("Azure worker concurrency = %+v", cfg.AzureWorker)
	}
	merged := Merge(cfg, &RepoConfig{})
	if merged.AzureWorker != cfg.AzureWorker {
		t.Fatalf("merged Azure worker config = %+v, want %+v", merged.AzureWorker, cfg.AzureWorker)
	}
}

func TestLoadGlobalAzureWorkerRejectsUnsafeOrUnboundedConfig(t *testing.T) {
	tests := []string{
		"enabled: true\n  runner_path: relative\n  config_path: /tmp/config\n",
		"enabled: true\n  runner_path: /tmp/runner\n  config_path: relative\n",
		"enabled: true\n  runner_path: /tmp/runner\n  config_path: /tmp/config\n  lease_duration: 30s\n  heartbeat_interval: 30s\n",
		"enabled: true\n  runner_path: /tmp/runner\n  config_path: /tmp/config\n  timeout: 25h\n",
		"enabled: true\n  runner_path: /tmp/runner\n  config_path: /tmp/config\n  review_concurrency: 0\n",
		"enabled: true\n  runner_path: /tmp/runner\n  config_path: /tmp/config\n  review_concurrency: 8\n  repair_concurrency: 8\n  test_concurrency: 1\n",
	}
	for _, block := range tests {
		if _, err := LoadGlobalFromBytes([]byte("agent: auto\nazure_worker:\n  " + block)); err == nil {
			t.Fatalf("unsafe Azure worker block accepted:\n%s", block)
		}
	}
}
