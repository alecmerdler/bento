package spicedb

import (
	"context"
	"fmt"
	"time"

	pb "github.com/authzed/authzed-go/proto/authzed/api/v1"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/warpstreamlabs/bento/public/service"
)

const checkTimeout = 30 * time.Second

func spiceDBCheckProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Checks a permission in SpiceDB based on input message fields").
		Description(`
This processor performs a SpiceDB CheckPermission call using values extracted from the input document.
It will query SpiceDB to determine if the specified subject has the requested permission on the resource.
`).
		Field(service.NewStringField("endpoint").
			Description("The SpiceDB server endpoint (host:port)").
			Example("localhost:50051")).
		Field(service.NewStringField("token").
			Description("The authentication token for SpiceDB").
			Secret()).
		Field(service.NewInterpolatedStringField("resource_type").
			Description("The resource type to check permission against").
			Example("document")).
		Field(service.NewInterpolatedStringField("resource_id").
			Description("The resource ID to check permission against").
			Example("document")).
		Field(service.NewInterpolatedStringField("subject_type").
			Description("The subject type to check").
			Example("user")).
		Field(service.NewInterpolatedStringField("subject_id").
			Description("The subject ID to check permission against").
			Example("document")).
		Field(service.NewInterpolatedStringField("permission").
			Description("The permission to check").
			Example("read")).
		Field(service.NewStringField("write_schema").
			Description(`The SpiceDB schema to write on start.`).
			Optional().
			Example(exampleWriteSchema)).
		// TODO(alecmerdler): Add support to specify the consistency level per request...
		Field(service.NewBoolField("full_consistency").
			Description("Whether to use full consistency for every check.").
			Default(false))
}

// SpiceDBCheckConfig holds configuration parameters for the SpiceDBCheck processor
type SpiceDBCheckConfig struct {
	ResourceType    *service.InterpolatedString `json:"resource_type" yaml:"resource_type"`
	ResourceID      *service.InterpolatedString `json:"resource_id" yaml:"resource_id"`
	SubjectType     *service.InterpolatedString `json:"subject_type" yaml:"subject_type"`
	SubjectID       *service.InterpolatedString `json:"subject_id" yaml:"subject_id"`
	PermissionName  *service.InterpolatedString `json:"permission" yaml:"permission"`
	FullConsistency bool                        `json:"full_consistency" yaml:"full_consistency"`
}

func init() {
	err := service.RegisterProcessor(
		"spicedb_check", spiceDBCheckProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newSpiceDBCheckProcessor(conf)
		},
	)
	if err != nil {
		panic(err)
	}
}

func newSpiceDBCheckProcessor(conf *service.ParsedConfig) (service.Processor, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, err
	}

	token, err := conf.FieldString("token")
	if err != nil {
		return nil, err
	}

	writeSchema, err := conf.FieldString("write_schema")
	if err != nil {
		return nil, err
	}

	resourceType, err := conf.FieldInterpolatedString("resource_type")
	if err != nil {
		return nil, err
	}

	resourceIDPath, err := conf.FieldInterpolatedString("resource_id")
	if err != nil {
		return nil, err
	}

	subjectType, err := conf.FieldInterpolatedString("subject_type")
	if err != nil {
		return nil, err
	}

	subjectID, err := conf.FieldInterpolatedString("subject_id")
	if err != nil {
		return nil, err
	}

	permission, err := conf.FieldInterpolatedString("permission")
	if err != nil {
		return nil, err
	}

	fullConsistency, err := conf.FieldBool("full_consistency")
	if err != nil {
		return nil, err
	}

	// TODO(alecmerdler): Add TLS fields to the config and pass them when creating client...
	transportCredentialsOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	tokenOption := grpcutil.WithInsecureBearerToken(token)
	client, err := authzed.NewClient(endpoint, tokenOption, transportCredentialsOption)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	if writeSchema != "" {
		if _, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{Schema: writeSchema}); err != nil {
			return nil, fmt.Errorf("failed to write schema: %w", err)
		}
	}

	config := &SpiceDBCheckConfig{
		ResourceType:    resourceType,
		ResourceID:      resourceIDPath,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		PermissionName:  permission,
		FullConsistency: fullConsistency,
	}

	return &spiceDBCheckProcessor{
		config: config,
		client: client,
	}, nil
}

type spiceDBCheckProcessor struct {
	config *SpiceDBCheckConfig
	client *authzed.Client
}

func (s *spiceDBCheckProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	resourceType, err := s.config.ResourceType.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource type: %v", err)
	}

	resourceID, err := s.config.ResourceID.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource ID: %v", err)
	}

	subjectType, err := s.config.SubjectType.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get subject type: %v", err)
	}

	subjectID, err := s.config.SubjectID.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get subject ID: %v", err)
	}

	permission, err := s.config.PermissionName.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to get permission name: %v", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	var consistency *v1.Consistency
	if s.config.FullConsistency {
		consistency = &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		}
	}

	resp, err := s.client.CheckPermission(reqCtx, &pb.CheckPermissionRequest{
		Resource: &pb.ObjectReference{
			ObjectType: resourceType,
			ObjectId:   resourceID,
		},
		Permission: permission,
		Subject: &pb.SubjectReference{
			Object: &pb.ObjectReference{
				ObjectType: subjectType,
				ObjectId:   subjectID,
			},
		},
		Consistency: consistency,
	})
	if err != nil {
		return nil, fmt.Errorf("SpiceDB permission check failed: %v", err)
	}

	var permissionship string
	switch resp.Permissionship {
	case pb.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION:
		permissionship = "GRANTED"
	case pb.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION:
		permissionship = "DENIED"
	default:
		permissionship = "UNKNOWN"
	}
	msg.SetStructured(CheckResult{Permissionship: permissionship})

	return service.MessageBatch{msg}, nil
}

func (s *spiceDBCheckProcessor) Close(ctx context.Context) error {
	return nil
}

type CheckResult struct {
	Permissionship string `json:"permissionship"`
}
