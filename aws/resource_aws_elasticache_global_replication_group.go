package aws

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/keyvaluetags"
)

func resourceAwsElasticacheGlobalReplicationGroup() *schema.Resource {
	//lintignore:R011
	return &schema.Resource{
		Create: resourceAwsElasticacheGlobalReplicationGroupCreate,
		Read:   resourceAwsElasticacheGlobalReplicationGroupRead,
		Update: resourceAwsElasticacheGlobalReplicationGroupUpdate,
		Delete: resourceAwsElasticacheGlobalReplicationGroupDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"global_replication_group_suffix": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"global_replication_group_description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			// every option below this point is actually input required to configure an ElastiCache
			// Replication Group. As that schema is updated, so should this one.
			"apply_immediately": {
				Type:     schema.TypeBool,
				Optional: true,
				Computed: true,
			},
			"at_rest_encryption_enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
				ForceNew: true,
			},
			"auth_token": {
				Type:         schema.TypeString,
				Optional:     true,
				Sensitive:    true,
				ForceNew:     true,
				ValidateFunc: validateAwsElastiCacheReplicationGroupAuthToken,
			},
			"auto_minor_version_upgrade": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"automatic_failover_enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"availability_zones": {
				Type:     schema.TypeSet,
				Optional: true,
				ForceNew: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			"cluster_mode": {
				Type:     schema.TypeList,
				Optional: true,
				// We allow Computed: true here since using number_cache_clusters
				// and a cluster mode enabled parameter_group_name will create
				// a single shard replication group with number_cache_clusters - 1
				// read replicas. Otherwise, the resource is marked ForceNew.
				Computed: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"replicas_per_node_group": {
							Type:     schema.TypeInt,
							Required: true,
							ForceNew: true,
						},
						"num_node_groups": {
							Type:     schema.TypeInt,
							Required: true,
						},
					},
				},
			},
			"configuration_endpoint_address": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"engine": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				Default:      "redis",
				ValidateFunc: validateElastiCacheGlobalReplicationGroupEngine,
			},
			"engine_version": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: validateElastiCacheGlobalReplicationGroupEngineVersion,
			},
			"maintenance_window": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
				StateFunc: func(val interface{}) string {
					// Elasticache always changes the maintenance
					// to lowercase
					return strings.ToLower(val.(string))
				},
				ValidateFunc: validateOnceAWeekWindowFormat,
			},
			"member_clusters": {
				Type:     schema.TypeSet,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			"node_type": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: validateElastiCacheGlobalReplicationGroupNodeType,
			},
			"notification_topic_arn": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validateArn,
			},
			"number_cache_clusters": {
				Type:     schema.TypeInt,
				Computed: true,
				Optional: true,
			},
			"parameter_group_name": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
			"port": {
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					// Suppress default memcached/redis ports when not defined
					if !d.IsNewResource() && new == "0" && (old == "6379" || old == "11211") {
						return true
					}
					return false
				},
			},
			"primary_endpoint_address": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"replication_group_description": {
				Type:     schema.TypeString,
				Required: true,
			},
			"replication_group_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.All(
					validation.StringLenBetween(1, 40),
					validation.StringMatch(regexp.MustCompile(`^[0-9a-zA-Z-]+$`), "must contain only alphanumeric characters and hyphens"),
					validation.StringMatch(regexp.MustCompile(`^[a-zA-Z]`), "must begin with a letter"),
					validation.StringDoesNotMatch(regexp.MustCompile(`--`), "cannot contain two consecutive hyphens"),
					validation.StringDoesNotMatch(regexp.MustCompile(`-$`), "cannot end with a hyphen"),
				),
				StateFunc: func(val interface{}) string {
					return strings.ToLower(val.(string))
				},
			},
			"security_group_names": {
				Type:     schema.TypeSet,
				Optional: true,
				Computed: true,
				ForceNew: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			"security_group_ids": {
				Type:     schema.TypeSet,
				Optional: true,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
			// A single-element string list containing an Amazon Resource Name (ARN) that
			// uniquely identifies a Redis RDB snapshot file stored in Amazon S3. The snapshot
			// file will be used to populate the node group.
			//
			// See also:
			// https://github.com/aws/aws-sdk-go/blob/4862a174f7fc92fb523fc39e68f00b87d91d2c3d/service/elasticache/api.go#L2079
			"snapshot_arns": {
				Type:     schema.TypeSet,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validateArn,
				},
				Set: schema.HashString,
			},
			"snapshot_retention_limit": {
				Type:         schema.TypeInt,
				Optional:     true,
				ValidateFunc: validation.IntAtMost(35),
			},
			"snapshot_window": {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ValidateFunc: validateOnceADayWindowFormat,
			},
			"snapshot_name": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"subnet_group_name": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
				ForceNew: true,
			},
			"tags": tagsSchema(),
			"transit_encryption_enabled": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
				ForceNew: true,
			},
			"kms_key_id": {
				Type:     schema.TypeString,
				ForceNew: true,
				Optional: true,
			},
		},
		SchemaVersion: 1,

		// SchemaVersion: 1 did not include any state changes via MigrateState.
		// Perform a no-operation state upgrade for Terraform 0.12 compatibility.
		// Future state migrations should be performed with StateUpgraders.
		MigrateState: func(v int, inst *terraform.InstanceState, meta interface{}) (*terraform.InstanceState, error) {
			return inst, nil
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Minute),
			Delete: schema.DefaultTimeout(40 * time.Minute),
			Update: schema.DefaultTimeout(40 * time.Minute),
		},
	}
}

func resourceAwsElasticacheGlobalReplicationGroupCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).elasticacheconn

	// first create the primary replication group
	primaryInput, err := elasticacheCreateReplicationGroupInputFromSchema(d)
	if err != nil {
		return err
	}

	primaryResp, err := conn.CreateReplicationGroup(primaryInput)
	if err != nil {
		return fmt.Errorf("Error creating Elasticache GlobalReplication Group: %w", err)
	}

	pending := []string{"creating", "modifying", "restoring", "snapshotting"}
	stateConf := &resource.StateChangeConf{
		Pending:    pending,
		Target:     []string{"available"},
		Refresh:    cacheGlobalReplicationGroupStateRefreshFunc(conn, d.Id(), pending),
		Timeout:    d.Timeout(schema.TimeoutCreate),
		MinTimeout: 10 * time.Second,
		Delay:      30 * time.Second,
	}

	log.Printf("[DEBUG] Waiting for state to become available: %v", d.Id())
	_, sterr := stateConf.WaitForState()
	if sterr != nil {
		return fmt.Errorf("Error waiting for elasticache replication group (%s) to be created: %s", d.Id(), sterr)
	}

	// now that the primary replication group is ready, create the global replication group
	globalInput := &elasticache.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix: aws.String(d.Get("global_replication_group_suffix").(string)),
		PrimaryReplicationGroupId:      primaryResp.ReplicationGroup.ReplicationGroupId,
	}

	if v, ok := d.GetOk("global_replication_group_description"); ok {
		globalInput.GlobalReplicationGroupDescription = aws.String(v.(string))
	}

	globalResp, err := conn.CreateGlobalReplicationGroup(globalInput)
	if err != nil {
		return err
	}

	d.SetId(aws.StringValue(globalResp.GlobalReplicationGroup.GlobalReplicationGroupId))

	return resourceAwsElasticacheGlobalReplicationGroupRead(d, meta)
}

func resourceAwsElasticacheGlobalReplicationGroupRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).elasticacheconn
	ignoreTagsConfig := meta.(*AWSClient).IgnoreTagsConfig

	req := &elasticache.DescribeGlobalReplicationGroupsInput{
		GlobalReplicationGroupId: aws.String(d.Id()),
	}

	res, err := conn.DescribeGlobalReplicationGroups(req)
	if err != nil {
		if isAWSErr(err, elasticache.ErrCodeGlobalReplicationGroupNotFoundFault, "") {
			log.Printf("[WARN] Elasticache GlobalReplication Group (%s) not found", d.Id())
			d.SetId("")
			return nil
		}

		return err
	}

	var rgp *elasticache.GlobalReplicationGroup
	for _, r := range res.GlobalReplicationGroups {
		if aws.StringValue(r.GlobalReplicationGroupId) == d.Id() {
			rgp = r
		}
	}

	if rgp == nil {
		log.Printf("[WARN] GlobalReplication Group (%s) not found", d.Id())
		d.SetId("")
		return nil
	}

	if aws.StringValue(rgp.Status) == "deleting" {
		log.Printf("[WARN] The GlobalReplication Group %q is currently in the `deleting` state", d.Id())
		d.SetId("")
		return nil
	}

	if rgp.AutomaticFailover != nil {
		switch strings.ToLower(aws.StringValue(rgp.AutomaticFailover)) {
		case elasticache.AutomaticFailoverStatusDisabled, elasticache.AutomaticFailoverStatusDisabling:
			d.Set("automatic_failover_enabled", false)
		case elasticache.AutomaticFailoverStatusEnabled, elasticache.AutomaticFailoverStatusEnabling:
			d.Set("automatic_failover_enabled", true)
		default:
			log.Printf("Unknown AutomaticFailover state %s", aws.StringValue(rgp.AutomaticFailover))
		}
	}

	d.Set("kms_key_id", rgp.KmsKeyId)

	d.Set("replication_group_description", rgp.Description)
	d.Set("number_cache_clusters", len(rgp.MemberClusters))
	if err := d.Set("member_clusters", flattenStringSet(rgp.MemberClusters)); err != nil {
		return fmt.Errorf("error setting member_clusters: %w", err)
	}
	if err := d.Set("cluster_mode", flattenElasticacheNodeGroupsToClusterMode(aws.BoolValue(rgp.ClusterEnabled), rgp.NodeGroups)); err != nil {
		return fmt.Errorf("error setting cluster_mode attribute: %w", err)
	}
	d.Set("replication_group_id", rgp.GlobalReplicationGroupId)

	if rgp.NodeGroups != nil {
		if len(rgp.NodeGroups[0].NodeGroupMembers) == 0 {
			return nil
		}

		cacheCluster := *rgp.NodeGroups[0].NodeGroupMembers[0]

		res, err := conn.DescribeCacheClusters(&elasticache.DescribeCacheClustersInput{
			CacheClusterId:    cacheCluster.CacheClusterId,
			ShowCacheNodeInfo: aws.Bool(true),
		})
		if err != nil {
			return err
		}

		if len(res.CacheClusters) == 0 {
			return nil
		}

		c := res.CacheClusters[0]
		d.Set("node_type", c.CacheNodeType)
		d.Set("engine", c.Engine)
		d.Set("engine_version", c.EngineVersion)
		d.Set("subnet_group_name", c.CacheSubnetGroupName)
		d.Set("security_group_names", flattenElastiCacheSecurityGroupNames(c.CacheSecurityGroups))
		d.Set("security_group_ids", flattenElastiCacheSecurityGroupIds(c.SecurityGroups))

		if c.CacheParameterGroup != nil {
			d.Set("parameter_group_name", c.CacheParameterGroup.CacheParameterGroupName)
		}

		d.Set("maintenance_window", c.PreferredMaintenanceWindow)
		d.Set("snapshot_window", rgp.SnapshotWindow)
		d.Set("snapshot_retention_limit", rgp.SnapshotRetentionLimit)

		if rgp.ConfigurationEndpoint != nil {
			d.Set("port", rgp.ConfigurationEndpoint.Port)
			d.Set("configuration_endpoint_address", rgp.ConfigurationEndpoint.Address)
		} else {
			d.Set("port", rgp.NodeGroups[0].PrimaryEndpoint.Port)
			d.Set("primary_endpoint_address", rgp.NodeGroups[0].PrimaryEndpoint.Address)
		}

		d.Set("auto_minor_version_upgrade", c.AutoMinorVersionUpgrade)
		d.Set("at_rest_encryption_enabled", c.AtRestEncryptionEnabled)
		d.Set("transit_encryption_enabled", c.TransitEncryptionEnabled)

		if c.AuthTokenEnabled != nil && !aws.BoolValue(c.AuthTokenEnabled) {
			d.Set("auth_token", nil)
		}

		arn := arn.ARN{
			Partition: meta.(*AWSClient).partition,
			Service:   "elasticache",
			Region:    meta.(*AWSClient).region,
			AccountID: meta.(*AWSClient).accountid,
			Resource:  fmt.Sprintf("cluster:%s", aws.StringValue(c.CacheClusterId)),
		}.String()

		tags, err := keyvaluetags.ElasticacheListTags(conn, arn)

		if err != nil {
			return fmt.Errorf("error listing tags for resource (%s): %w", arn, err)
		}

		if err := d.Set("tags", tags.IgnoreAws().IgnoreConfig(ignoreTagsConfig).Map()); err != nil {
			return fmt.Errorf("error setting tags: %w", err)
		}
	}

	return nil
}

func resourceAwsElasticacheGlobalReplicationGroupUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).elasticacheconn

	if d.HasChange("cluster_mode.0.num_node_groups") {
		o, n := d.GetChange("cluster_mode.0.num_node_groups")
		oldNumNodeGroups := o.(int)
		newNumNodeGroups := n.(int)

		input := &elasticache.ModifyGlobalReplicationGroupShardConfigurationInput{
			ApplyImmediately:         aws.Bool(true),
			NodeGroupCount:           aws.Int64(int64(newNumNodeGroups)),
			GlobalReplicationGroupId: aws.String(d.Id()),
		}

		if oldNumNodeGroups > newNumNodeGroups {
			// Node Group IDs are 1 indexed: 0001 through 0015
			// Loop from highest old ID until we reach highest new ID
			nodeGroupsToRemove := []string{}
			for i := oldNumNodeGroups; i > newNumNodeGroups; i-- {
				nodeGroupID := fmt.Sprintf("%04d", i)
				nodeGroupsToRemove = append(nodeGroupsToRemove, nodeGroupID)
			}
			input.NodeGroupsToRemove = aws.StringSlice(nodeGroupsToRemove)
		}

		log.Printf("[DEBUG] Modifying Elasticache GlobalReplication Group (%s) shard configuration: %s", d.Id(), input)
		_, err := conn.ModifyGlobalReplicationGroupShardConfiguration(input)
		if err != nil {
			return fmt.Errorf("error modifying Elasticache GlobalReplication Group shard configuration: %w", err)
		}

		err = waitForModifyElasticacheGlobalReplicationGroup(conn, d.Id(), d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf("error waiting for Elasticache GlobalReplication Group (%s) shard reconfiguration completion: %w", d.Id(), err)
		}
	}

	if d.HasChange("number_cache_clusters") {
		o, n := d.GetChange("number_cache_clusters")
		oldNumberCacheClusters := o.(int)
		newNumberCacheClusters := n.(int)

		// We will try to use similar naming to the console, which are 1 indexed: RGID-001 through RGID-006
		var addClusterIDs, removeClusterIDs []string
		for clusterID := oldNumberCacheClusters + 1; clusterID <= newNumberCacheClusters; clusterID++ {
			addClusterIDs = append(addClusterIDs, fmt.Sprintf("%s-%03d", d.Id(), clusterID))
		}
		for clusterID := oldNumberCacheClusters; clusterID >= (newNumberCacheClusters + 1); clusterID-- {
			removeClusterIDs = append(removeClusterIDs, fmt.Sprintf("%s-%03d", d.Id(), clusterID))
		}

		if len(addClusterIDs) > 0 {
			// Kick off all the Cache Cluster creations
			for _, cacheClusterID := range addClusterIDs {
				input := &elasticache.CreateCacheClusterInput{
					CacheClusterId:           aws.String(cacheClusterID),
					GlobalReplicationGroupId: aws.String(d.Id()),
				}
				_, err := createElasticacheCacheCluster(conn, input)
				if err != nil {
					// Future enhancement: we could retry creation with random ID on naming collision
					// if isAWSErr(err, elasticache.ErrCodeCacheClusterAlreadyExistsFault, "") { ... }
					return fmt.Errorf("error creating Elasticache Cache Cluster (adding replica): %w", err)
				}
			}

			// Wait for all Cache Cluster creations
			for _, cacheClusterID := range addClusterIDs {
				err := waitForCreateElasticacheCacheCluster(conn, cacheClusterID, d.Timeout(schema.TimeoutUpdate))
				if err != nil {
					return fmt.Errorf("error waiting for Elasticache Cache Cluster (%s) to be created (adding replica): %w", cacheClusterID, err)
				}
			}
		}

		if len(removeClusterIDs) > 0 {
			// Cannot reassign primary cluster ID while automatic failover is enabled
			// If we temporarily disable automatic failover, ensure we re-enable it
			reEnableAutomaticFailover := false

			// Kick off all the Cache Cluster deletions
			for _, cacheClusterID := range removeClusterIDs {
				err := deleteElasticacheCacheCluster(conn, cacheClusterID)
				if err != nil {
					// Future enhancement: we could retry deletion with random existing ID on missing name
					// if isAWSErr(err, elasticache.ErrCodeCacheClusterNotFoundFault, "") { ... }
					if !isAWSErr(err, elasticache.ErrCodeInvalidCacheClusterStateFault, "serving as primary") {
						return fmt.Errorf("error deleting Elasticache Cache Cluster (%s) (removing replica): %w", cacheClusterID, err)
					}

					// Use GlobalReplication Group MemberClusters to find a new primary cache cluster ID
					// that is not in removeClusterIDs
					newPrimaryClusterID := ""

					describeGlobalReplicationGroupInput := &elasticache.DescribeGlobalReplicationGroupsInput{
						GlobalReplicationGroupId: aws.String(d.Id()),
					}
					log.Printf("[DEBUG] Reading Elasticache GlobalReplication Group: %s", describeGlobalReplicationGroupInput)
					output, err := conn.DescribeGlobalReplicationGroups(describeGlobalReplicationGroupInput)
					if err != nil {
						return fmt.Errorf("error reading Elasticache GlobalReplication Group (%s) to determine new primary: %w", d.Id(), err)
					}
					if output == nil || len(output.GlobalReplicationGroups) == 0 || len(output.GlobalReplicationGroups[0].MemberClusters) == 0 {
						return fmt.Errorf("error reading Elasticache GlobalReplication Group (%s) to determine new primary: missing replication group information", d.Id())
					}

					for _, memberClusterPtr := range output.GlobalReplicationGroups[0].MemberClusters {
						memberCluster := aws.StringValue(memberClusterPtr)
						memberClusterInRemoveClusterIDs := false
						for _, removeClusterID := range removeClusterIDs {
							if memberCluster == removeClusterID {
								memberClusterInRemoveClusterIDs = true
								break
							}
						}
						if !memberClusterInRemoveClusterIDs {
							newPrimaryClusterID = memberCluster
							break
						}
					}
					if newPrimaryClusterID == "" {
						return fmt.Errorf("error reading Elasticache GlobalReplication Group (%s) to determine new primary: unable to assign new primary", d.Id())
					}

					// Disable automatic failover if enabled
					// Must be applied previous to trying to set new primary
					// InvalidGlobalReplicationGroupState: Cannot manually promote a new master cache cluster while autofailover is enabled
					if aws.StringValue(output.GlobalReplicationGroups[0].AutomaticFailover) == elasticache.AutomaticFailoverStatusEnabled {
						// Be kind and rewind
						if d.Get("automatic_failover_enabled").(bool) {
							reEnableAutomaticFailover = true
						}

						modifyGlobalReplicationGroupInput := &elasticache.ModifyGlobalReplicationGroupInput{
							ApplyImmediately:         aws.Bool(true),
							AutomaticFailoverEnabled: aws.Bool(false),
							GlobalReplicationGroupId: aws.String(d.Id()),
						}
						log.Printf("[DEBUG] Modifying Elasticache GlobalReplication Group: %s", modifyGlobalReplicationGroupInput)
						_, err = conn.ModifyGlobalReplicationGroup(modifyGlobalReplicationGroupInput)
						if err != nil {
							return fmt.Errorf("error modifying Elasticache GlobalReplication Group (%s) to set new primary: %sw", d.Id(), err)
						}
						err = waitForModifyElasticacheGlobalReplicationGroup(conn, d.Id(), d.Timeout(schema.TimeoutUpdate))
						if err != nil {
							return fmt.Errorf("error waiting for Elasticache GlobalReplication Group (%s) to be available: %w", d.Id(), err)
						}
					}

					// Set new primary
					modifyGlobalReplicationGroupInput := &elasticache.ModifyGlobalReplicationGroupInput{
						ApplyImmediately:         aws.Bool(true),
						PrimaryClusterId:         aws.String(newPrimaryClusterID),
						GlobalReplicationGroupId: aws.String(d.Id()),
					}
					log.Printf("[DEBUG] Modifying Elasticache GlobalReplication Group: %s", modifyGlobalReplicationGroupInput)
					_, err = conn.ModifyGlobalReplicationGroup(modifyGlobalReplicationGroupInput)
					if err != nil {
						return fmt.Errorf("error modifying Elasticache GlobalReplication Group (%s) to set new primary: %w", d.Id(), err)
					}
					err = waitForModifyElasticacheGlobalReplicationGroup(conn, d.Id(), d.Timeout(schema.TimeoutUpdate))
					if err != nil {
						return fmt.Errorf("error waiting for Elasticache GlobalReplication Group (%s) to be available: %w", d.Id(), err)
					}

					// Finally retry deleting the cache cluster
					err = deleteElasticacheCacheCluster(conn, cacheClusterID)
					if err != nil {
						return fmt.Errorf("error deleting Elasticache Cache Cluster (%s) (removing replica after setting new primary): %w", cacheClusterID, err)
					}
				}
			}

			// Wait for all Cache Cluster deletions
			for _, cacheClusterID := range removeClusterIDs {
				err := waitForDeleteElasticacheCacheCluster(conn, cacheClusterID, d.Timeout(schema.TimeoutUpdate))
				if err != nil {
					return fmt.Errorf("error waiting for Elasticache Cache Cluster (%s) to be deleted (removing replica): %w", cacheClusterID, err)
				}
			}

			// Re-enable automatic failover if we needed to temporarily disable it
			if reEnableAutomaticFailover {
				input := &elasticache.ModifyGlobalReplicationGroupInput{
					ApplyImmediately:         aws.Bool(true),
					AutomaticFailoverEnabled: aws.Bool(true),
					GlobalReplicationGroupId: aws.String(d.Id()),
				}
				log.Printf("[DEBUG] Modifying Elasticache GlobalReplication Group: %s", input)
				_, err := conn.ModifyGlobalReplicationGroup(input)
				if err != nil {
					return fmt.Errorf("error modifying Elasticache GlobalReplication Group (%s) to re-enable automatic failover: %w", d.Id(), err)
				}
			}
		}
	}

	requestUpdate := false
	params := &elasticache.ModifyGlobalReplicationGroupInput{
		ApplyImmediately:         aws.Bool(d.Get("apply_immediately").(bool)),
		GlobalReplicationGroupId: aws.String(d.Id()),
	}

	if d.HasChange("replication_group_description") {
		params.GlobalReplicationGroupDescription = aws.String(d.Get("replication_group_description").(string))
		requestUpdate = true
	}

	if d.HasChange("automatic_failover_enabled") {
		params.AutomaticFailoverEnabled = aws.Bool(d.Get("automatic_failover_enabled").(bool))
		requestUpdate = true
	}

	if d.HasChange("auto_minor_version_upgrade") {
		params.AutoMinorVersionUpgrade = aws.Bool(d.Get("auto_minor_version_upgrade").(bool))
		requestUpdate = true
	}

	if d.HasChange("security_group_ids") {
		if attr := d.Get("security_group_ids").(*schema.Set); attr.Len() > 0 {
			params.SecurityGroupIds = expandStringSet(attr)
			requestUpdate = true
		}
	}

	if d.HasChange("security_group_names") {
		if attr := d.Get("security_group_names").(*schema.Set); attr.Len() > 0 {
			params.CacheSecurityGroupNames = expandStringSet(attr)
			requestUpdate = true
		}
	}

	if d.HasChange("maintenance_window") {
		params.PreferredMaintenanceWindow = aws.String(d.Get("maintenance_window").(string))
		requestUpdate = true
	}

	if d.HasChange("notification_topic_arn") {
		params.NotificationTopicArn = aws.String(d.Get("notification_topic_arn").(string))
		requestUpdate = true
	}

	if d.HasChange("parameter_group_name") {
		params.CacheParameterGroupName = aws.String(d.Get("parameter_group_name").(string))
		requestUpdate = true
	}

	if d.HasChange("engine_version") {
		params.EngineVersion = aws.String(d.Get("engine_version").(string))
		requestUpdate = true
	}

	if d.HasChange("snapshot_retention_limit") {
		// This is a real hack to set the Snapshotting Cluster ID to be the first Cluster in the RG
		o, _ := d.GetChange("snapshot_retention_limit")
		if o.(int) == 0 {
			params.SnapshottingClusterId = aws.String(fmt.Sprintf("%s-001", d.Id()))
		}

		params.SnapshotRetentionLimit = aws.Int64(int64(d.Get("snapshot_retention_limit").(int)))
		requestUpdate = true
	}

	if d.HasChange("snapshot_window") {
		params.SnapshotWindow = aws.String(d.Get("snapshot_window").(string))
		requestUpdate = true
	}

	if d.HasChange("node_type") {
		params.CacheNodeType = aws.String(d.Get("node_type").(string))
		requestUpdate = true
	}

	if requestUpdate {
		_, err := conn.ModifyGlobalReplicationGroup(params)
		if err != nil {
			return fmt.Errorf("error updating Elasticache GlobalReplication Group (%s): %w", d.Id(), err)
		}

		err = waitForModifyElasticacheGlobalReplicationGroup(conn, d.Id(), d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf("error waiting for Elasticache GlobalReplication Group (%s) to be updated: %w", d.Id(), err)
		}
	}

	if d.HasChange("tags") {
		clusters := d.Get("member_clusters").(*schema.Set).List()

		for _, cluster := range clusters {

			arn := arn.ARN{
				Partition: meta.(*AWSClient).partition,
				Service:   "elasticache",
				Region:    meta.(*AWSClient).region,
				AccountID: meta.(*AWSClient).accountid,
				Resource:  fmt.Sprintf("cluster:%s", cluster),
			}.String()

			o, n := d.GetChange("tags")
			if err := keyvaluetags.ElasticacheUpdateTags(conn, arn, o, n); err != nil {
				return fmt.Errorf("error updating tags: %w", err)
			}
		}
	}

	return resourceAwsElasticacheGlobalReplicationGroupRead(d, meta)
}

func resourceAwsElasticacheGlobalReplicationGroupDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).elasticacheconn

	err := deleteElasticacheGlobalReplicationGroup(d.Id(), conn)
	if err != nil {
		return fmt.Errorf("error deleting Elasticache GlobalReplication Group (%s): %w", d.Id(), err)
	}

	return nil
}

func cacheGlobalReplicationGroupStateRefreshFunc(conn *elasticache.ElastiCache, replicationGroupId string, pending []string) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		resp, err := conn.DescribeGlobalReplicationGroups(&elasticache.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: aws.String(replicationGroupId),
		})
		if err != nil {
			if isAWSErr(err, elasticache.ErrCodeGlobalReplicationGroupNotFoundFault, "") {
				log.Printf("[DEBUG] GlobalReplication Group Not Found")
				return nil, "", nil
			}

			log.Printf("[ERROR] cacheClusterGlobalReplicationGroupStateRefreshFunc: %s", err)
			return nil, "", err
		}

		if len(resp.GlobalReplicationGroups) == 0 {
			return nil, "", fmt.Errorf("Error: no Cache GlobalReplication Groups found for id (%s)", replicationGroupId)
		}

		var rg *elasticache.GlobalReplicationGroup
		for _, replicationGroup := range resp.GlobalReplicationGroups {
			rgId := aws.StringValue(replicationGroup.GlobalReplicationGroupId)
			if rgId == replicationGroupId {
				log.Printf("[DEBUG] Found matching ElastiCache GlobalReplication Group: %s", rgId)
				rg = replicationGroup
			}
		}

		if rg == nil {
			return nil, "", fmt.Errorf("Error: no matching ElastiCache GlobalReplication Group for id (%s)", replicationGroupId)
		}

		log.Printf("[DEBUG] ElastiCache GlobalReplication Group (%s) status: %v", replicationGroupId, aws.StringValue(rg.Status))

		// return the current state if it's in the pending array
		for _, p := range pending {
			log.Printf("[DEBUG] ElastiCache: checking pending state (%s) for GlobalReplication Group (%s), GlobalReplication Group status: %s", pending, replicationGroupId, aws.StringValue(rg.Status))
			s := aws.StringValue(rg.Status)
			if p == s {
				log.Printf("[DEBUG] Return with status: %v", aws.StringValue(rg.Status))
				return s, p, nil
			}
		}

		return rg, aws.StringValue(rg.Status), nil
	}
}

func deleteElasticacheGlobalReplicationGroup(replicationGroupID string, conn *elasticache.ElastiCache) error {
	input := &elasticache.DeleteGlobalReplicationGroupInput{
		GlobalReplicationGroupId: aws.String(replicationGroupID),
	}

	// 10 minutes should give any creating/deleting cache clusters or snapshots time to complete
	err := resource.Retry(10*time.Minute, func() *resource.RetryError {
		_, err := conn.DeleteGlobalReplicationGroup(input)
		if err != nil {
			if isAWSErr(err, elasticache.ErrCodeGlobalReplicationGroupNotFoundFault, "") {
				return nil
			}
			// Cache Cluster is creating/deleting or GlobalReplication Group is snapshotting
			// InvalidGlobalReplicationGroupState: Cache cluster tf-acc-test-uqhe-003 is not in a valid state to be deleted
			if isAWSErr(err, elasticache.ErrCodeInvalidGlobalReplicationGroupStateFault, "") {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	if isResourceTimeoutError(err) {
		_, err = conn.DeleteGlobalReplicationGroup(input)
	}

	if isAWSErr(err, elasticache.ErrCodeGlobalReplicationGroupNotFoundFault, "") {
		return nil
	}

	if err != nil {
		return fmt.Errorf("error deleting Elasticache GlobalReplication Group: %w", err)
	}

	log.Printf("[DEBUG] Waiting for deletion: %s", replicationGroupID)
	stateConf := &resource.StateChangeConf{
		Pending:    []string{"creating", "available", "deleting"},
		Target:     []string{},
		Refresh:    cacheGlobalReplicationGroupStateRefreshFunc(conn, replicationGroupID, []string{}),
		Timeout:    40 * time.Minute,
		MinTimeout: 10 * time.Second,
		Delay:      30 * time.Second,
	}

	_, err = stateConf.WaitForState()
	return err
}

func flattenElasticacheNodeGroupsToClusterMode(clusterEnabled bool, nodeGroups []*elasticache.NodeGroup) []map[string]interface{} {
	if !clusterEnabled {
		return []map[string]interface{}{}
	}

	m := map[string]interface{}{
		"num_node_groups":         0,
		"replicas_per_node_group": 0,
	}

	if len(nodeGroups) == 0 {
		return []map[string]interface{}{m}
	}

	m["num_node_groups"] = len(nodeGroups)
	m["replicas_per_node_group"] = (len(nodeGroups[0].NodeGroupMembers) - 1)
	return []map[string]interface{}{m}
}

func waitForModifyElasticacheGlobalReplicationGroup(conn *elasticache.ElastiCache, replicationGroupID string, timeout time.Duration) error {
	pending := []string{"creating", "modifying", "snapshotting"}
	stateConf := &resource.StateChangeConf{
		Pending:    pending,
		Target:     []string{"available"},
		Refresh:    cacheGlobalReplicationGroupStateRefreshFunc(conn, replicationGroupID, pending),
		Timeout:    timeout,
		MinTimeout: 10 * time.Second,
		Delay:      30 * time.Second,
	}

	log.Printf("[DEBUG] Waiting for Elasticache GlobalReplication Group (%s) to become available", replicationGroupID)
	_, err := stateConf.WaitForState()
	return err
}

func validateElastiCacheGlobalReplicationGroupEngine(v interface{}, k string) (ws []string, errors []error) {
	if strings.ToLower(v.(string)) != "redis" {
		errors = append(errors, fmt.Errorf("The only acceptable Engine type when using GlobalReplication Groups is Redis"))
	}
	return
}

func validateElastiCacheGlobalReplicationGroupEngineVersion(v interface{}, k string) (ws []string, errors []error) {
	parts := strings.Split(v.(string), ".")
	if len(parts) < 1 {
		errors = append(errors, fmt.Errorf("The Engine version should either ommitted, or not be empty"))
		return
	}
	if parts[0] != "5" || parts[0] != "6" {
		errors = append(errors, fmt.Errorf("Global Replication Groups require engine versions of Redis >= 5.0.6"))
	}
	return
}

func validateElastiCacheGlobalReplicationGroupNodeType(v interface{}, k string) (ws []string, errors []error) {
	parts := strings.Split(v.(string), ".")
	if len(parts) < 2 {
		errors = append(errors, fmt.Errorf("The node type is required"))
		return
	}
	if parts[1] != "r5" || parts[1] != "m5" {
		errors = append(errors, fmt.Errorf("Global Replication Groups require either r5 or m5 node types"))
	}
	return
}
