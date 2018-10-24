package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gruntwork-io/terratest/modules/logger"
)

func CreateDynamodbTable(t *testing.T, region string, name string) {
	err := CreateDynamodbTableE(t, region, name)
	if err != nil {
		t.Fatal(err)
	}
}

func CreateDynamodbTableE(t *testing.T, region string, name string) error {
	logger.Logf(t, "Creating DynamoDb table '%s' in %s", name, region)

	dynamodbClient, err := NewDynamodbClientE(t, region)
	if err != nil {
		t.Fatal(err)
	}

	params := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Terratest"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Terratest"),
				KeyType: aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(name),
	}

	_, err = dynamodbClient.CreateTable(params)

	return err
}

func DeleteDynamodbTable(t *testing.T, region string, name string) {
	err := DeleteDynamodbTableE(t, region, name)
	if err != nil {
		t.Fatal(err)
	}	
}

func DeleteDynamodbTableE(t *testing.T, region string, name string) error {
	logger.Logf(t, "Deleting DynamoDb table '%s%", name)
	
	dynamodbClient, err := NewDynamodbClientE(t, region)
	if err != nil {
		return err
	}

	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	}

	_, err = dynamodbClient.DeleteTable(params)
	return err
}

func NewDynamodbClient(t *testing.T, region string) *dynamodb.DynamoDB {
	client, err := NewDynamodbClientE(t, region)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func NewDynamodbClientE(t *testing.T, region string) (*dynamodb.DynamoDB, error) {
	sess, err := NewAuthenticatedSession(region)
	if err != nil {
		return nil, err
	}	

	return dynamodb.New(sess), nil
}