# BaseHarvestTaskChain([BaseTaskChain](./base.md)) | `harvest`
The base class for all Harvest Task Chains. This class is responsible for managing the Harvest persistence silo 
(typically `harvest-core`) and is the parent class for all Harvest Task Chains.

> This class does not perform any data collection. It only serves as a parent class for Harvest Task Chains which *do*
> perform data collection, such as the `aws` Task Chain available in the [AWS Plugin](https://github.com/Cloud-Harvest/CloudHarvestPluginAws). 

## Directives

| Directive                | Required | Default        | Description                                                                                            |
|--------------------------|----------|----------------|--------------------------------------------------------------------------------------------------------|
| `platform`               | Yes      | None           | The Platform (ie AWS, Azure, Google)                                                                   |
| `service`                | Yes      | None           | The Platform's service name (ie RDS, EC2, GCP)                                                         |
| `type`                   | Yes      | None           | The Service subtype, if applicable (ie RDS instance, EC2 event)                                        |
| `account`                | Yes      | None           | The Platform account name or identifier                                                                |
| `region`                 | Yes      | None           | The geographic region name for the Platform                                                            |
| `unique_identifier_keys` | Yes      | None           | The unique filter keys for the harvested data                                                          |
| `destination_silo`       | No       | `harvest-core` | The name of the destination silo where the harvested data will be stored                               |
| `extra_metadata_fields`  | No       | None           | Additional metadata fields to include in the harvested data's metadata record                          |
| `mode`                   | No       | 'all'          | The mode of the harvest task chain. 'all' will harvest all data, 'single' will harvest a single record |

## Example
```yaml
harvest:
  name: RDS Instances
  description: Harvest all RDS instances from AWS
  platform: AWS
  service: RDS
  type: instance
  account: my-aws-account
  region: us-west-2
  unique_identifier_keys:
    - DbInstanceArn
  mode: all    
```
