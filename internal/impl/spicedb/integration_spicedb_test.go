package spicedb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

const gracefulShutdown = 15 * time.Second

func TestIntegrationSpiceDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "quay.io/authzed/spicedb",
		Tag:          "latest",
		Cmd:          []string{"serve"},
		ExposedPorts: []string{"50051/tcp"},
		Env: []string{
			"SPICEDB_GRPC_PRESHARED_KEY=secretkey",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		transportCredentialsOption := grpc.WithTransportCredentials(insecure.NewCredentials())
		tokenOption := grpcutil.WithInsecureBearerToken("secretkey")
		client, err := authzed.NewClient(fmt.Sprintf("localhost:%v", resource.GetPort("50051/tcp")), tokenOption, transportCredentialsOption)
		if err != nil {
			return err
		}

		_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
		if err != nil && !strings.Contains(err.Error(), "No schema has been defined") {
			return err
		}

		return client.Close()
	}))

	outputConf := fmt.Sprintf(`
spicedb:
  endpoint: localhost:%s
  token: secretkey
  write_schema: |
    definition user {}

    definition document {
      relation owner: user

      permission view = owner
    }
  args_mapping: |
    root = {
      "resource": {
        "object_type": this.resourceType,
        "object_id": this.resourceId
      },
      "relation": this.relation,
      "subject": {
        "object": {
          "object_type": "user",
          "object_id": this.userId
        }		
      }
    }
`, resource.GetPort("50051/tcp"))

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))
	require.NoError(t, sb.AddOutputYAML(outputConf))

	inFn, err := sb.AddBatchProducerFunc()
	require.NoError(t, err)

	stream, err := sb.Build()
	require.NoError(t, err)

	go func() {
		require.NoError(t, stream.Run(context.Background()))
	}()

	var batch service.MessageBatch
	msg := &inputMessage{
		ResourceType: "document",
		ResourceID:   "firstdoc",
		Relation:     "owner",
		UserID:       "bob",
	}
	raw, err := json.Marshal(msg)
	require.NoError(t, err)

	batch = append(batch, service.NewMessage(raw))

	require.NoError(t, inFn(context.Background(), batch))
	require.NoError(t, stream.StopWithin(gracefulShutdown))
}

type inputMessage struct {
	ResourceType string `json:"resourceType"`
	ResourceID   string `json:"resourceId"`
	Relation     string `json:"relation"`
	UserID       string `json:"userId"`
}
