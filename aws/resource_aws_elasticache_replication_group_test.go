package aws

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func init() {
	resource.AddTestSweepers("aws_elasticache_replication_group", &resource.Sweeper{
		Name: "aws_elasticache_replication_group",
		F:    testSweepElasticacheGlobalReplicationGroups,
	})
}

func testSweepElasticacheGlobalReplicationGroups(region string) error {
	client, err := sharedClientForRegion(region)
	if err != nil {
		return fmt.Errorf("error getting client: %s", err)
	}
	conn := client.(*AWSClient).elasticacheconn

	err = conn.DescribeGlobalReplicationGroupsPages(&elasticache.DescribeGlobalReplicationGroupsInput{}, func(page *elasticache.DescribeGlobalReplicationGroupsOutput, isLast bool) bool {
		if len(page.GlobalReplicationGroups) == 0 {
			log.Print("[DEBUG] No Elasticache Replicaton Groups to sweep")
			return false
		}

		for _, replicationGroup := range page.GlobalReplicationGroups {
			id := aws.StringValue(replicationGroup.GlobalReplicationGroupId)

			log.Printf("[INFO] Deleting Elasticache GlobalReplication Group: %s", id)
			err := deleteElasticacheGlobalReplicationGroup(id, conn)
			if err != nil {
				log.Printf("[ERROR] Failed to delete Elasticache GlobalReplication Group (%s): %s", id, err)
			}
		}
		return !isLast
	})
	if err != nil {
		if testSweepSkipSweepError(err) {
			log.Printf("[WARN] Skipping Elasticache GlobalReplication Group sweep for %s: %s", region, err)
			return nil
		}
		return fmt.Errorf("Error retrieving Elasticache GlobalReplication Groups: %s", err)
	}
	return nil
}

func TestAccAWSElasticacheGlobalReplicationGroup_basic(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "cluster_mode.#", "0"),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "member_clusters.#", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "auto_minor_version_upgrade", "false"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"}, //not in the API
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_Uppercase(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_Uppercase(strings.ToUpper(rName)),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "replication_group_id", rName),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_updateDescription(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "replication_group_description", "test description"),
					resource.TestCheckResourceAttr(
						resourceName, "auto_minor_version_upgrade", "false"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedDescription(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "replication_group_description", "updated description"),
					resource.TestCheckResourceAttr(
						resourceName, "auto_minor_version_upgrade", "true"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_updateMaintenanceWindow(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "maintenance_window", "tue:06:30-tue:07:30"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedMaintenanceWindow(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "maintenance_window", "wed:03:00-wed:06:00"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_updateNodeSize(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "node_type", "cache.t3.small"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedNodeSize(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "node_type", "cache.t3.medium"),
				),
			},
		},
	})
}

//This is a test to prove that we panic we get in https://github.com/hashicorp/terraform/issues/9097
func TestAccAWSElasticacheGlobalReplicationGroup_updateParameterGroup(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	parameterGroupResourceName1 := "aws_elasticache_parameter_group.test.0"
	parameterGroupResourceName2 := "aws_elasticache_parameter_group.test.1"
	resourceName := "aws_elasticache_replication_group.test"
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigParameterGroupName(rName, 0),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttrPair(resourceName, "parameter_group_name", parameterGroupResourceName1, "name"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigParameterGroupName(rName, 1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttrPair(resourceName, "parameter_group_name", parameterGroupResourceName2, "name"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_vpc(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupInVPCConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "1"),
					resource.TestCheckResourceAttr(
						resourceName, "auto_minor_version_upgrade", "false"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately", "availability_zones"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_multiAzInVpc(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupMultiAZInVPCConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "automatic_failover_enabled", "true"),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_window", "02:00-03:00"),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_retention_limit", "7"),
					resource.TestCheckResourceAttrSet(
						resourceName, "primary_endpoint_address"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately", "availability_zones"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_redisClusterInVpc2(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupRedisClusterInVPCConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(
						resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_window", "02:00-03:00"),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_retention_limit", "7"),
					resource.TestCheckResourceAttrSet(
						resourceName, "primary_endpoint_address"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately", "availability_zones"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_ClusterMode_Basic(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterConfig(rName, 2, 1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "4"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.num_node_groups", "2"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.replicas_per_node_group", "1"),
					resource.TestCheckResourceAttr(resourceName, "port", "6379"),
					resource.TestCheckResourceAttrSet(resourceName, "configuration_endpoint_address"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_ClusterMode_NumNodeGroups(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterConfig(rName, 3, 1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "6"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.num_node_groups", "3"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.replicas_per_node_group", "1"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterConfig(rName, 1, 1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "2"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.num_node_groups", "1"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.replicas_per_node_group", "1"),
				),
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterConfig(rName, 2, 1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "4"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.num_node_groups", "2"),
					resource.TestCheckResourceAttr(resourceName, "cluster_mode.0.replicas_per_node_group", "1"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_clusteringAndCacheNodesCausesError(t *testing.T) {
	rInt := acctest.RandInt()
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config:      testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterErrorConfig(rInt, rName),
				ExpectError: regexp.MustCompile("Either `number_cache_clusters` or `cluster_mode` must be set"),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_enableSnapshotting(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_retention_limit", "0"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigEnableSnapshotting(rName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "snapshot_retention_limit", "2"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_enableAuthTokenTransitEncryption(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroup_EnableAuthTokenTransitEncryptionConfig(acctest.RandInt(), acctest.RandString(10), acctest.RandString(16)),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "transit_encryption_enabled", "true"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately", "auth_token", "availability_zones"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_enableAtRestEncryption(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroup_EnableAtRestEncryptionConfig(acctest.RandInt(), acctest.RandString(10)),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(
						resourceName, "at_rest_encryption_enabled", "true"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately", "availability_zones"},
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_useCmkKmsKeyId(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroup_UseCmkKmsKeyId(acctest.RandInt(), acctest.RandString(10)),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists("aws_elasticache_replication_group.bar", &rg),
					resource.TestCheckResourceAttrSet("aws_elasticache_replication_group.bar", "kms_key_id"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_NumberCacheClusters(t *testing.T) {
	var replicationGroup elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 2, false),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "2"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 4, false),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "4"),
				),
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 2, false),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "2"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_NumberCacheClusters_Failover_AutoFailoverDisabled(t *testing.T) {
	var replicationGroup elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 3, false),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "3"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"},
			},
			{
				PreConfig: func() {
					// Simulate failover so primary is on node we are trying to delete
					conn := testAccProvider.Meta().(*AWSClient).elasticacheconn
					input := &elasticache.ModifyGlobalReplicationGroupInput{
						ApplyImmediately:         aws.Bool(true),
						PrimaryClusterId:         aws.String(fmt.Sprintf("%s-003", rName)),
						GlobalReplicationGroupId: aws.String(rName),
					}
					if _, err := conn.ModifyGlobalReplicationGroup(input); err != nil {
						t.Fatalf("error setting new primary cache cluster: %s", err)
					}
					if err := waitForModifyElasticacheGlobalReplicationGroup(conn, rName, 40*time.Minute); err != nil {
						t.Fatalf("error waiting for new primary cache cluster: %s", err)
					}
				},
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 2, false),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "false"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "2"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_NumberCacheClusters_Failover_AutoFailoverEnabled(t *testing.T) {
	var replicationGroup elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 3, true),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "true"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "3"),
				),
			},
			{
				PreConfig: func() {
					// Simulate failover so primary is on node we are trying to delete
					conn := testAccProvider.Meta().(*AWSClient).elasticacheconn

					// Must disable automatic failover first
					var input *elasticache.ModifyGlobalReplicationGroupInput = &elasticache.ModifyGlobalReplicationGroupInput{
						ApplyImmediately:         aws.Bool(true),
						AutomaticFailoverEnabled: aws.Bool(false),
						GlobalReplicationGroupId: aws.String(rName),
					}
					if _, err := conn.ModifyGlobalReplicationGroup(input); err != nil {
						t.Fatalf("error disabling automatic failover: %s", err)
					}
					if err := waitForModifyElasticacheGlobalReplicationGroup(conn, rName, 40*time.Minute); err != nil {
						t.Fatalf("error waiting for disabling automatic failover: %s", err)
					}

					// Failover
					input = &elasticache.ModifyGlobalReplicationGroupInput{
						ApplyImmediately:         aws.Bool(true),
						PrimaryClusterId:         aws.String(fmt.Sprintf("%s-003", rName)),
						GlobalReplicationGroupId: aws.String(rName),
					}
					if _, err := conn.ModifyGlobalReplicationGroup(input); err != nil {
						t.Fatalf("error setting new primary cache cluster: %s", err)
					}
					if err := waitForModifyElasticacheGlobalReplicationGroup(conn, rName, 40*time.Minute); err != nil {
						t.Fatalf("error waiting for new primary cache cluster: %s", err)
					}

					// Re-enable automatic failover like nothing ever happened
					input = &elasticache.ModifyGlobalReplicationGroupInput{
						ApplyImmediately:         aws.Bool(true),
						AutomaticFailoverEnabled: aws.Bool(true),
						GlobalReplicationGroupId: aws.String(rName),
					}
					if _, err := conn.ModifyGlobalReplicationGroup(input); err != nil {
						t.Fatalf("error enabled automatic failover: %s", err)
					}
					if err := waitForModifyElasticacheGlobalReplicationGroup(conn, rName, 40*time.Minute); err != nil {
						t.Fatalf("error waiting for enabled automatic failover: %s", err)
					}
				},
				Config: testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName, 2, true),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &replicationGroup),
					resource.TestCheckResourceAttr(resourceName, "automatic_failover_enabled", "true"),
					resource.TestCheckResourceAttr(resourceName, "number_cache_clusters", "2"),
				),
			},
		},
	})
}

func TestAccAWSElasticacheGlobalReplicationGroup_tags(t *testing.T) {
	var rg elasticache.GlobalReplicationGroup
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_elasticache_replication_group.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSElasticacheGlobalReplicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigTags1(rName, "key1", "value1"),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "tags.%", "1"),
					resource.TestCheckResourceAttr(resourceName, "tags.key1", "value1"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"apply_immediately"}, //not in the API
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigTags2(rName, "key1", "value1updated", "key2", "value2"),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "tags.%", "2"),
					resource.TestCheckResourceAttr(resourceName, "tags.key1", "value1updated"),
					resource.TestCheckResourceAttr(resourceName, "tags.key2", "value2"),
				),
			},
			{
				Config: testAccAWSElasticacheGlobalReplicationGroupConfigTags1(rName, "key2", "value2"),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSElasticacheGlobalReplicationGroupExists(resourceName, &rg),
					resource.TestCheckResourceAttr(resourceName, "tags.%", "1"),
					resource.TestCheckResourceAttr(resourceName, "tags.key2", "value2"),
				),
			},
		},
	})
}

func TestResourceAWSElastiCacheGlobalReplicationGroupEngineValidation(t *testing.T) {
	cases := []struct {
		Value    string
		ErrCount int
	}{
		{
			Value:    "Redis",
			ErrCount: 0,
		},
		{
			Value:    "REDIS",
			ErrCount: 0,
		},
		{
			Value:    "memcached",
			ErrCount: 1,
		},
	}

	for _, tc := range cases {
		_, errors := validateAwsElastiCacheGlobalReplicationGroupEngine(tc.Value, "aws_elasticache_replication_group_engine")

		if len(errors) != tc.ErrCount {
			t.Fatalf("Expected the ElastiCache GlobalReplication Group Engine to trigger a validation error")
		}
	}
}

func testAccCheckAWSElasticacheGlobalReplicationGroupExists(n string, v *elasticache.GlobalReplicationGroup) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No replication group ID is set")
		}

		conn := testAccProvider.Meta().(*AWSClient).elasticacheconn
		res, err := conn.DescribeGlobalReplicationGroups(&elasticache.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: aws.String(rs.Primary.ID),
		})
		if err != nil {
			return fmt.Errorf("Elasticache error: %v", err)
		}

		for _, rg := range res.GlobalReplicationGroups {
			if *rg.GlobalReplicationGroupId == rs.Primary.ID {
				*v = *rg
			}
		}

		return nil
	}
}

func testAccCheckAWSElasticacheGlobalReplicationDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*AWSClient).elasticacheconn

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aws_elasticache_replication_group" {
			continue
		}
		res, err := conn.DescribeGlobalReplicationGroups(&elasticache.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: aws.String(rs.Primary.ID),
		})
		if err != nil {
			// Verify the error is what we want
			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "GlobalReplicationGroupNotFoundFault" {
				continue
			}
			return err
		}
		if len(res.GlobalReplicationGroups) > 0 {
			return fmt.Errorf("still exist.")
		}
	}
	return nil
}

func testAccAWSElasticacheGlobalReplicationGroupConfig(rName string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = false
  maintenance_window            = "tue:06:30-tue:07:30"
  snapshot_window               = "01:00-02:00"
}
`, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupConfig_Uppercase(rName string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-number-cache-clusters"
  }
}

resource "aws_subnet" "test" {
  count = 2

  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = "192.168.${count.index}.0/24"
  vpc_id            = aws_vpc.test.id

  tags = {
    Name = "tf-acc-elasticache-replication-group-number-cache-clusters"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name       = %[1]q
  subnet_ids = aws_subnet.test[*].id
}

resource "aws_elasticache_replication_group" "test" {
  node_type                     = "cache.t2.micro"
  number_cache_clusters         = 1
  port                          = 6379
  replication_group_description = "test description"
  replication_group_id          = %[1]q
  subnet_group_name             = aws_elasticache_subnet_group.test.name
}
`, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigEnableSnapshotting(rName string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = false
  maintenance_window            = "tue:06:30-tue:07:30"
  snapshot_window               = "01:00-02:00"
  snapshot_retention_limit      = 2
}
`, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigParameterGroupName(rName string, parameterGroupNameIndex int) string {
	return fmt.Sprintf(`
resource "aws_elasticache_parameter_group" "test" {
  count = 2

  # We do not have a data source for "latest" Elasticache family
  # so unfortunately we must hardcode this for now
  family = "redis5.0"

  name = "%[1]s-${count.index}"

  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }
}

resource "aws_elasticache_replication_group" "test" {
  apply_immediately             = true
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  parameter_group_name          = aws_elasticache_parameter_group.test.*.name[%[2]d]
  replication_group_description = "test description"
  replication_group_id          = %[1]q
}
`, rName, parameterGroupNameIndex)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedDescription(rName string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "updated description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = true
}
`, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedMaintenanceWindow(rName string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "updated description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = true
  maintenance_window            = "wed:03:00-wed:06:00"
  snapshot_window               = "01:00-02:00"
}
`, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigUpdatedNodeSize(rName string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "updated description"
  node_type                     = "cache.t3.medium"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
}
`, rName)
}

var testAccAWSElasticacheGlobalReplicationGroupInVPCConfig = fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-in-vpc"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-in-vpc"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"
  subnet_ids  = [aws_subnet.test.id]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 1
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  availability_zones            = [data.aws_availability_zones.available.names[0]]
  auto_minor_version_upgrade    = false
}
`, acctest.RandInt(), acctest.RandInt(), acctest.RandString(10))

var testAccAWSElasticacheGlobalReplicationGroupMultiAZInVPCConfig = fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-multi-az-in-vpc"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-multi-az-in-vpc-foo"
  }
}

resource "aws_subnet" "test2" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.16.0/20"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "tf-acc-elasticache-replication-group-multi-az-in-vpc-bar"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"
  subnet_ids = [
    aws_subnet.test.id,
    aws_subnet.test2.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  availability_zones            = [data.aws_availability_zones.available.names[0], data.aws_availability_zones.available.names[1]]
  automatic_failover_enabled    = true
  snapshot_window               = "02:00-03:00"
  snapshot_retention_limit      = 7
}
`, acctest.RandInt(), acctest.RandInt(), acctest.RandString(10))

var testAccAWSElasticacheGlobalReplicationGroupRedisClusterInVPCConfig = fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-redis-cluster-in-vpc"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-redis-cluster-in-vpc-foo"
  }
}

resource "aws_subnet" "test2" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.16.0/20"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "tf-acc-elasticache-replication-group-redis-cluster-in-vpc-bar"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"
  subnet_ids = [
    aws_subnet.test.id,
    aws_subnet.test2.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t3.medium"
  number_cache_clusters         = "2"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  availability_zones            = [data.aws_availability_zones.available.names[0], data.aws_availability_zones.available.names[1]]
  automatic_failover_enabled    = false
  snapshot_window               = "02:00-03:00"
  snapshot_retention_limit      = 7
  engine_version                = "3.2.4"
  maintenance_window            = "thu:03:00-thu:04:00"
}
`, acctest.RandInt(), acctest.RandInt(), acctest.RandString(10))

func testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterErrorConfig(rInt int, rName string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-native-redis-cluster-err"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-native-redis-cluster-err-test"
  }
}

resource "aws_subnet" "test2" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.16.0/20"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "tf-acc-elasticache-replication-group-native-redis-cluster-err-test"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"

  subnet_ids = [
    aws_subnet.test.id,
    aws_subnet.test.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t2.micro"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  automatic_failover_enabled    = true

  cluster_mode {
    replicas_per_node_group = 1
    num_node_groups         = 2
  }

  number_cache_clusters = 3
}
`, rInt, rInt, rName)
}

func testAccAWSElasticacheGlobalReplicationGroupNativeRedisClusterConfig(rName string, numNodeGroups, replicasPerNodeGroup int) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-native-redis-cluster"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-native-redis-cluster-test"
  }
}

resource "aws_subnet" "test2" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.16.0/20"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "tf-acc-elasticache-replication-group-native-redis-cluster-test"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-%[1]s"
  description = "tf-test-cache-subnet-group-descr"

  subnet_ids = [
    aws_subnet.test.id,
    aws_subnet.test.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-%[1]s"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%[1]s"
  replication_group_description = "test description"
  node_type                     = "cache.t2.micro"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  automatic_failover_enabled    = true

  cluster_mode {
    num_node_groups         = %d
    replicas_per_node_group = %d
  }
}
`, rName, numNodeGroups, replicasPerNodeGroup)
}

func testAccAWSElasticacheGlobalReplicationGroup_UseCmkKmsKeyId(rInt int, rString string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "foo" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-at-rest-encryption"
  }
}

resource "aws_subnet" "foo" {
  vpc_id            = aws_vpc.foo.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-at-rest-encryption"
  }
}

resource "aws_elasticache_subnet_group" "bar" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"

  subnet_ids = [
    aws_subnet.foo.id,
  ]
}

resource "aws_security_group" "bar" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.foo.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_kms_key" "bar" {
  description = "tf-test-cmk-kms-key-id"
}

resource "aws_elasticache_replication_group" "bar" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t2.micro"
  number_cache_clusters         = "1"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.bar.name
  security_group_ids            = [aws_security_group.bar.id]
  parameter_group_name          = "default.redis3.2"
  availability_zones            = [data.aws_availability_zones.available.names[0]]
  engine_version                = "3.2.6"
  at_rest_encryption_enabled    = true
  kms_key_id                    = aws_kms_key.bar.arn
}
`, rInt, rInt, rString)
}

func testAccAWSElasticacheGlobalReplicationGroup_EnableAtRestEncryptionConfig(rInt int, rString string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-at-rest-encryption"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-at-rest-encryption"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"

  subnet_ids = [
    aws_subnet.test.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t2.micro"
  number_cache_clusters         = "1"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  parameter_group_name          = "default.redis3.2"
  availability_zones            = [data.aws_availability_zones.available.names[0]]
  engine_version                = "3.2.6"
  at_rest_encryption_enabled    = true
}
`, rInt, rInt, rString)
}

func testAccAWSElasticacheGlobalReplicationGroup_EnableAuthTokenTransitEncryptionConfig(rInt int, rString10 string, rString16 string) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-auth-token-transit-encryption"
  }
}

resource "aws_subnet" "test" {
  vpc_id            = aws_vpc.test.id
  cidr_block        = "192.168.0.0/20"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "tf-acc-elasticache-replication-group-auth-token-transit-encryption"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name        = "tf-test-cache-subnet-%03d"
  description = "tf-test-cache-subnet-group-descr"

  subnet_ids = [
    aws_subnet.test.id,
  ]
}

resource "aws_security_group" "test" {
  name        = "tf-test-security-group-%03d"
  description = "tf-test-security-group-descr"
  vpc_id      = aws_vpc.test.id

  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = "tf-%s"
  replication_group_description = "test description"
  node_type                     = "cache.t2.micro"
  number_cache_clusters         = "1"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.test.name
  security_group_ids            = [aws_security_group.test.id]
  parameter_group_name          = "default.redis3.2"
  availability_zones            = [data.aws_availability_zones.available.names[0]]
  engine_version                = "3.2.6"
  transit_encryption_enabled    = true
  auth_token                    = "%s"
}
`, rInt, rInt, rString10, rString16)
}

func testAccAWSElasticacheGlobalReplicationGroupConfig_NumberCacheClusters(rName string, numberCacheClusters int, autoFailover bool) string {
	return fmt.Sprintf(`
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "aws_vpc" "test" {
  cidr_block = "192.168.0.0/16"

  tags = {
    Name = "terraform-testacc-elasticache-replication-group-number-cache-clusters"
  }
}

resource "aws_subnet" "test" {
  count = 2

  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = "192.168.${count.index}.0/24"
  vpc_id            = aws_vpc.test.id

  tags = {
    Name = "tf-acc-elasticache-replication-group-number-cache-clusters"
  }
}

resource "aws_elasticache_subnet_group" "test" {
  name       = "%[1]s"
  subnet_ids = aws_subnet.test[*].id
}

resource "aws_elasticache_replication_group" "test" {
  # InvalidParameterCombination: Automatic failover is not supported for T1 and T2 cache node types.
  automatic_failover_enabled    = %[2]t
  node_type                     = "cache.t3.medium"
  number_cache_clusters         = %[3]d
  replication_group_id          = "%[1]s"
  replication_group_description = "Terraform Acceptance Testing - number_cache_clusters"
  subnet_group_name             = aws_elasticache_subnet_group.test.name
}
`, rName, autoFailover, numberCacheClusters)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigTags1(rName, tagKey1, tagValue1 string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = false
  maintenance_window            = "tue:06:30-tue:07:30"
  snapshot_window               = "01:00-02:00"

  tags = {
    %[2]q = %[3]q
  }
}
`, rName, tagKey1, tagValue1)
}

func testAccAWSElasticacheGlobalReplicationGroupConfigTags2(rName, tagKey1, tagValue1, tagKey2, tagValue2 string) string {
	return fmt.Sprintf(`
resource "aws_elasticache_replication_group" "test" {
  replication_group_id          = %[1]q
  replication_group_description = "test description"
  node_type                     = "cache.t3.small"
  number_cache_clusters         = 2
  port                          = 6379
  apply_immediately             = true
  auto_minor_version_upgrade    = false
  maintenance_window            = "tue:06:30-tue:07:30"
  snapshot_window               = "01:00-02:00"

  tags = {
    %[2]q = %[3]q
    %[4]q = %[5]q
  }
}
`, rName, tagKey1, tagValue1, tagKey2, tagValue2)
}
