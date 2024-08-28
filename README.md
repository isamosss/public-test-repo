# Simplifying Automotive Damage Processing with Amazon Bedrock and Vector Databases

Assessing the costs related to repairing damages on assets is a challenge that all industries have, in particular automotive companies have had this challenge for a long time. 

There are multiple market solution that leverage Machine Learning processes targeted on addressing this challenge. 

The proposal of this solution is to offer a new method levearging GenAI and MultiModal Vector Databases.

# Important Information about this solution and Quick Demo:

1. The images contained on this solution are all from open source data sets that can be found [here](https://universe.roboflow.com/car-damage-kadad/car-damage-images/).
2. The data set used for this solution, has been broken down to specific cars makes and models, even thought the images are not of those specific models. The idea was to create separate metdata to demonstrate the solution and the use case.
3. Below is a quick demo of how the user interacts with this solution:


<video width="320" height="240" controls>
  <source src="static_assets/demo.mov" type="video/mp4">
</video>

# cool-beans

static_assets/demo.mov

Press play to see the video.

[![](static_assets/demo.mov)]

[![Sol Arch](/static_assets/demo.mov)]

# Solution Details:

The solution involves two important parts, the ingestion and the inference. Below is the architecture:

![Sol Arch](/static_assets/damage_repair_sol.png)  

Data Ingestion Flow Steps:

1. The Ingestion Processor will start by getting data from our current data set and it is going to run through Amazon Bedrock Anthropic Claude 3 Haiku. In this step the output is a standardized metadata which contains detailed information about the current damage, this includes make, model, year, location, labor cost, parts associated with the damage, labour hours required for repair, area of the damage and other details that are important.
2. The Ingestion Processor will send both the current damage image and the output of Step 1 to Amazon Bedrock Titan Multimodal Embeddings. In this step the output is a vector representation of the metadata and the image. 
3. The Ingestion Process will pick up the vector and store that vector in Amazon OpenSearch Vector Database, this data being stored on the Vector Database will also contain the plain text metadata from Step 1. 
4. The Ingestion Process will then store the raw image into S3, this will then be used by the inference flow to pull the images and show the matches to the user.

Inference Flow Steps:

1. The user will interact with the UI running on the Image Process, that interaction includes providing the image of a new car damage and some basic information about that damage. The Image Process will grab that information and send to Amazon Bedrock Anthropic Claude 3 Haiku and create a metadata from this new damage as close as possible in format to the metadata that was create on Step 1 as part of the ingestion process. This will make is so that when finding the closest matches the accuracy will be as high as possible.
2. The inference processor will send both the current damage image and the output of Step 5 to Amazon Bedrock Titan Multimodal Embeddings. In this step the output is a vector representation of the metadata and the image. 
3. The Inference Processor component will use the vector to do a similarity search on the Vector Database and find the closest 3 matches.
4. With the closest matches from the Vector Database, the inference processor will use the plain text data that was store with the vector to do a basic calculator of average cost from those closest 3 matches. 
5. The Front End will pull the images from S3 to show it in the UI. The UI will also show the accuracy, the image of each match and the metadata that was stored on each match. 

# Solution Deployment Requirements:

Before deploying the solution, make sure the following requirements have been met.

## Requirement 1: Enable Amazon Bedrock Models

Go To the Bedrock console in one of the Bedrock supported regions and enable at least the following models:

- Amazon Titan Multimoal Embeddings
- Anthropic Claude 3 Haiku

In order to enable the mentioned models. You can follow the instructions provided [here](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html#model-access-modify).

## Requirement 2: Install AWS CLI

Make sure you have the latest version of the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) running on your machine.

## Requirement 3: Appropriate IAM Permissions to deploy the solution and access required services

An IAM User/Role that can run CloudFormation templates and has necessary access to create resources for ECS, IAM, Systems Manager, OpenSearch Serverless, S3, CloudFront and Application Load Balancers.

# Deployment steps:

Once the requirements have been met, the following steps can be followed to deploy the solution.

## Step 1: Run CloudFormation template

Choose from one of the following deployment regions, right now this solution can only run on regions where bedrock is supported.

CloudFormation Deployments
| Region | CloudFormation Link |
| :---: | ---: |
| US-EAST-1 | [![Open In CloudFormation](/static_assets/view-template.png)](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/quickcreate?templateURL=https://s3.us-east-1.amazonaws.com/pedroni-us-east-1/new_build_proj.yml)|
| US-EAST-2 | [![Open In CloudFormation](/static_assets/view-template.png)](https://us-east-2.console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/quickcreate?templateURL=https://s3.us-east-1.amazonaws.com/pedroni-us-east-1/new_build_proj.yml)|
| US-WEST-2 | [![Open In CloudFormation](/static_assets/view-template.png)](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/quickcreate?templateURL=https://s3.us-east-1.amazonaws.com/pedroni-us-east-1/new_build_proj.yml)|
| EU-CENTRAL-1 | [![Open In CloudFormation](/static_assets/view-template.png)](https://eu-central-1.console.aws.amazon.com/cloudformation/home?region=eu-central-1#/stacks/quickcreate?templateURL=https://s3.us-east-1.amazonaws.com/pedroni-us-east-1/new_build_proj.yml)|

## Step 2: Download the data set from this [repository](https://universe.roboflow.com/car-damage-kadad/car-damage-images/). 

In order to download this data set you may need to create a free user and then download the data set.
The screenshot below shows the format and the option to choose when downloading. Once the zip file is downloaded you can extract to a local folder.

![Download](/static_assets/data_set_download.png)  

## Step 3: Upload Data Set to Source S3 Bucket

Run the following commands to upload the required damage images to the S3 Bucket. You can find the bucket name by going to the stack outputs and looking at the 'SourceS3Bucket' value.

```
aws s3 cp /path/to/source/folder/train/ s3://{source-bucket-name}  --recursive --exclude "*" --include "*.jpg" --include "*.png"
aws s3 cp /path/to/source/folder/valid/ s3://{source-bucket-name} --recursive --exclude "*" --include "*.jpg" --include "*.png"
```

## Step 4: Run Ingestion Task

In this step, the task which will be ingesting the content from the source s3 bucket will be initiated. In order to do that run the following sequence of commands.

```
aws ssm get-parameters --names /car-repair/security-group --query 'Parameters[0].Value'
aws ssm get-parameters --names /car-repair/subnet --profile --query 'Parameters[0].Value' 
```

These two commands above will retrieve the id for the subnet and the security groups that are required for the next command. 
Copy the command below and before running it, replace the 'security-group-id' and 'subnet-id' with the respective values retrieved in the previous commands and then run the command.

```
aws ecs run-task --task-definition ingestion-definition --cluster damage-ecs-cluster --network-configuration '{ "awsvpcConfiguration": { "assignPublicIp":"ENABLED", "securityGroups": ["security-group-id"], "subnets": ["subnet-id"]}}' --launch-type="FARGATE" --region us-west-2 --profile Admin2
```

After running this command, you can navigate to your Amazon ECS Console, open your ECS Cluster and you should see two tasks running, one task runs from the inference taks definition and the one we just created runs from the ingration task definition. The ingestion task will be a temporary task which will be loading the data from the Source S3 Bucket into the Open Search Vector Database.

## Step 5: Access Inference Code.

Once the CloudFormation stack has finished deploying, go to the outputs tab, look for the "InferenceUIURL" key, you should see a cloudfront distribution link, click there should take you to the inference ui.

![CFN Outputs](/static_assets/cfn_output_1.png)

## Step 6: Testing the Solution:

In the repository, there is a "test_data_set" folder. This folder has random images which can be used for testing the solution. Follow the steps below in order to see the results in the UI.

1. In the UI, take the following actions and load the any of the images from the test data set:

2. On the left side of the UI, choose the parameters, Make, model, area of the damage, type of damage, the severity and how many matches you want to find from the Vector Database.

![Test1_Results](/static_assets/test_1_example.png)

In this example, image was loaded, and 3 matches were found. 

3. In this test, 3 matches were found. As we can see from the images, they were close damages, and the solution used the metadata stored to calculate the average. 

> [!NOTE] 
> The "Match Accuracy" shown for each image is an indication of how close the vectors from our current image and the stored ones are. As metadata is changed the accuracy of the matches is going to change as well.

4. Now let's see how changing the options from the user changes the accuracy of the results.

![Test1_Results](/static_assets/test_2_example.png)

5. As the image above shows, changing the parameters, but loading the same image, provided different results. The search matched with the same images but the accuracy is different. This indicates that the parameters chosen were closer to the metadata of the ingested images, thus influencing the vector created on the ingestion process.

6. Play around with the images in the 'test_data_set' folder, or even try some images from the data set we loaded into the Vector DB.

7. Under the image upload button, there will be the JSON metadata created by Claude for the metadata stored in OpenSearch alongside the vector. We can use that to compare how the images were ingested and how close the metadatas are.

# Cleanup Process:

If you would like to cleanup this solution from your AWS Account follow the steps below:

1. Open your CloudFormation Console, click on the stack that was deployed and go to Outputs. There you should see the name of your ECR Repository and the S3 Bucket Names for both bucket S3BucketFrontEnd and SourceS3Bucket.

2. Go to the S3 console, find each bucket and delete all the content in each bucket. The buckets should be empty, otherwise the CloudFormation stack will fail to delete it.

3. Go to the ECR console, find the ecr repository and delete all images in this repository. The reposiroty should be empty, otherwise the CloudFormation stack will fail to delete it.

4. Start the deletion of the CloudFormation Stack. This is going to remove all the other resources from the AWS Account.