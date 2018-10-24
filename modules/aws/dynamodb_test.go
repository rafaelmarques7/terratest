package aws

import (
	"strings"
	"testing"
	"time"

	"github.com/gruntwork-io/terratest/modules/retry"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/gruntwork-io/terratest/modules/random"
)

func TestCreateAndDestroyDynamodbTable(t *testing.T) {
	t.Parallel()

	// Setup input args
	id := random.UniqueId()
	region := GetRandomRegion(t, nil, nil)
	dynamodbTableName := "gruntwork-terratest-" + strings.ToLower(id)
	logger.Logf(t, "Random values selected for table ''. Region = %s, Id = %s\n", dynamodbTableName, region, id)

	// Create Dynamodb table. May take some time.
	CreateDynamodbTable(t, region, dynamodbTableName)

	// The deletion of the table must be in a retry loop, as its creation may take some time.
	maxRetries := 30
	timeBetweenRetries := 5 * time.Second
	description := "Delete Dynamodb table"

	retry.DoWithRetry(t, description, maxRetries, timeBetweenRetries, func() (string, error) {
		err := DeleteDynamodbTableE(t, region, dynamodbTableName)
		if err != nil {
			return "", err 
		}
		return "", nil
	})
}