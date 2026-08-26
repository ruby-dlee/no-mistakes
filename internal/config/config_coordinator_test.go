package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorDefaultsStayLocalAndDisabled(t *testing.T) {
	cfg, err := LoadGlobalFromBytes([]byte("{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Coordinator.Enabled {
		t.Fatal("coordinator must remain opt-in")
	}
	if cfg.Coordinator.ListenAddress != "127.0.0.1:9783" ||
		cfg.Coordinator.GitHubWebhookSecretEnv != "NO_MISTAKES_GITHUB_WEBHOOK_SECRET" ||
		cfg.Coordinator.BatchSize != 100 || cfg.Coordinator.MaxConcurrency != 4 ||
		cfg.Coordinator.ReconcileInterval != time.Minute {
		t.Fatalf("coordinator defaults = %+v", cfg.Coordinator)
	}
}

func TestCoordinatorLoadsOnlyFromTrustedGlobalConfig(t *testing.T) {
	cfg, err := LoadGlobalFromBytes([]byte(`coordinator:
  enabled: true
  listen_address: "0.0.0.0:8088"
  github_webhook_secret_env: "NM_TEST_WEBHOOK_SECRET"
  reconcile_interval: "15s"
  batch_size: 12
  max_concurrency: 3
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Coordinator.Enabled || cfg.Coordinator.ListenAddress != "0.0.0.0:8088" ||
		cfg.Coordinator.GitHubWebhookSecretEnv != "NM_TEST_WEBHOOK_SECRET" ||
		cfg.Coordinator.ReconcileInterval != 15*time.Second || cfg.Coordinator.BatchSize != 12 ||
		cfg.Coordinator.MaxConcurrency != 3 {
		t.Fatalf("coordinator config = %+v", cfg.Coordinator)
	}
	if _, exists := reflect.TypeOf(RepoConfig{}).FieldByName("Coordinator"); exists {
		t.Fatal("repository configuration exposes the global-only coordinator")
	}
}

func TestCoordinatorRejectsUnsafeOrUnboundedConfiguration(t *testing.T) {
	tests := []string{
		"coordinator:\n  enabled: true\n  listen_address: not-an-address\n",
		"coordinator:\n  enabled: true\n  github_webhook_secret_env: bad-name\n",
		"coordinator:\n  reconcile_interval: 500ms\n",
		"coordinator:\n  reconcile_interval: 25h\n",
		"coordinator:\n  batch_size: 0\n",
		"coordinator:\n  max_concurrency: 17\n",
		"coordinator:\n  github_webhook_secret_env: " + strings.Repeat("A", 65) + "\n",
	}
	for _, input := range tests {
		if _, err := LoadGlobalFromBytes([]byte(input)); err == nil {
			t.Fatalf("unsafe coordinator config accepted:\n%s", input)
		}
	}
}
