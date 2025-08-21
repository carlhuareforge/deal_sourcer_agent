#!/usr/bin/env python3
"""
Quick script to check S3 bucket setup and verify permissions
"""
import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get S3 configuration
S3_BUCKET = os.getenv('S3_BUCKET', '')
S3_REGION = os.getenv('S3_REGION', 'us-east-1')

if not S3_BUCKET:
    print("❌ S3_BUCKET not configured in .env file")
    exit(1)

print(f"🔍 Checking S3 bucket: {S3_BUCKET}")
print(f"   Region: {S3_REGION}")
print("-" * 50)

try:
    # Initialize S3 client
    s3 = boto3.client('s3', region_name=S3_REGION)
    
    # 1. Check if bucket exists and we have access
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print("✅ Bucket exists and is accessible")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print("❌ Bucket does not exist")
        elif error_code == '403':
            print("❌ Access denied to bucket")
        else:
            print(f"❌ Error accessing bucket: {e}")
        exit(1)
    
    # 2. List existing objects to see current structure
    print("\n📂 Current bucket contents:")
    response = s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=20)
    
    if 'Contents' in response:
        # Group by prefix
        root_files = []
        follower_counts_files = []
        other_files = []
        
        for obj in response['Contents']:
            key = obj['Key']
            size_mb = obj['Size'] / 1024 / 1024
            if key.startswith('follower_counts/'):
                follower_counts_files.append(f"   - {key} ({size_mb:.2f} MB)")
            elif '/' not in key:
                root_files.append(f"   - {key} ({size_mb:.2f} MB)")
            else:
                other_files.append(f"   - {key} ({size_mb:.2f} MB)")
        
        if root_files:
            print("\n   Root level:")
            for f in root_files[:5]:
                print(f)
            if len(root_files) > 5:
                print(f"   ... and {len(root_files)-5} more files")
        
        if follower_counts_files:
            print("\n   follower_counts/ directory:")
            for f in follower_counts_files[:5]:
                print(f)
            if len(follower_counts_files) > 5:
                print(f"   ... and {len(follower_counts_files)-5} more files")
        else:
            print("\n   ℹ️  No follower_counts/ directory yet (will be created on first upload)")
        
        if other_files:
            print("\n   Other directories:")
            for f in other_files[:5]:
                print(f)
            if len(other_files) > 5:
                print(f"   ... and {len(other_files)-5} more files")
    else:
        print("   (Empty bucket)")
    
    # 3. Test write permissions
    print("\n🔐 Testing permissions:")
    test_key = "test_write_permission.txt"
    try:
        # Try to upload a test file
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=test_key,
            Body=b"test",
            Metadata={'test': 'true'}
        )
        print("   ✅ Write permission: OK")
        
        # Clean up test file
        s3.delete_object(Bucket=S3_BUCKET, Key=test_key)
        print("   ✅ Delete permission: OK")
    except ClientError as e:
        print(f"   ❌ Write/Delete permission error: {e}")
    
    # 4. Check if we can create the follower_counts prefix
    print("\n📝 Pre-flight check for follower_counts sync:")
    test_key = "follower_counts/test.txt"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=test_key,
            Body=b"test"
        )
        print("   ✅ Can create follower_counts/ prefix")
        s3.delete_object(Bucket=S3_BUCKET, Key=test_key)
    except ClientError as e:
        print(f"   ❌ Cannot create follower_counts/ prefix: {e}")
    
    print("\n✅ S3 bucket is ready for use!")
    print("\n📌 Notes:")
    print("   - The follower_counts/ directory will be created automatically on first sync")
    print("   - Your database file will be stored at the root level")
    print("   - Make sure your AWS credentials have s3:GetObject, s3:PutObject, and s3:ListBucket permissions")
    
except NoCredentialsError:
    print("❌ AWS credentials not found. Please configure:")
    print("   - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env, or")
    print("   - Run 'aws configure' to set up AWS CLI credentials")
except Exception as e:
    print(f"❌ Unexpected error: {e}")