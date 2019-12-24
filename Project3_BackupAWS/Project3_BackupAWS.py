import boto3
import botocore
import os

def getBucketName():
    bucketName = input("Enter Bucket Name (New or Existing): ")
    return bucketName

def checkForBucket(bucketName):
    bucketFound = False
    for bucket in s3.buckets.all():
        if(bucket.name == bucketName):
            bucketFound = True
    return bucketFound

def backupNewDirectory(bucketName):
    print("Attemping to create bucket ", bucketName)
    try:
        s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={"LocationConstraint":"ap-northeast-1"})
    except:
        print("Invalid bucket name, follow proper naming syntax")
        return

    with open("metadata.txt", "w+") as metafile:
        #dirPathBackup = "C:\\Users\\Jacob\\Pictures"
        rootDir = os.getcwd()
        #traverses all the directories
        for dirName, subdirList, fileList in os.walk(rootDir):       
            for fname in fileList:
                path = dirName + "\\" + fname
                if(fname == "desktop.ini"):
                    fname = "_%Folder%_"
                    path = dirName + "\\" + fname
                    print("Putting: " + path  + "\n")
                    s3.Object(bucketName, path).put(Body= dirName)
                    fileStat = os.stat(dirName + "\\")
                    metafile.write(path + " " + format(fileStat.st_mtime) + "\n")
                else:
                    print("Putting: " + path  + "\n")
                    s3.Object(bucketName, path).put(Body=open(path, "rb"))
                    fileStat = os.stat(dirName + "\\" + fname)                    
                    metafile.write(path + " " + format(fileStat.st_mtime) + "\n")         
    
    s3.Object(bucketName, "metadata.txt").put(Body=open("metadata.txt", "rb"))      
    print("Finished backup process at %s" % bucketName)

def backupExistingDirectory(bucketName):
    #if they choose a bucket that already exists, but is empty, this will create the appropriate meta file
    try:
        metafile = s3.Object(bucketName, "metadata.txt")
        metafile = metafile.get()['Body'].read().decode('utf-8')
    except:
        with open("metadata.txt", "w+") as metafile:
            metafile.write("")
        s3.Object(bucketName, "metadata.txt").put(Body=open("metadata.txt", "rb"))
        metafile = s3.Object(bucketName, "metadata.txt")
        metafile = metafile.get()['Body'].read().decode('utf-8')

    #dirPathBackup = "C:\\Users\\Jacob\\Pictures"
    rootDir = os.getcwd()

    #the metadata file is rewritten each time for simplicity
    with open("metadata.txt", "w+") as new_metafile:
        #traverses all the directories
        for dirName, subdirList, fileList in os.walk(rootDir):
                for fname in fileList:
                    path = dirName + "\\" + fname             
                    #empty folder
                    if(fname == "desktop.ini"):
                        fname = "_%Folder%_"    
                        path = dirName + "\\" + fname
                        fileStat = os.stat(dirName + "\\")
                        new_metafile.write(path + " " + format(fileStat.st_mtime) + "\n")
                        #check if folder has been modified
                        if (checkForChange(metafile, dirName + "\\", path)):
                            continue
                        else:
                            #Folder has been modified, replace the s3 object                      
                            print("Change detected or new file: " + path)
                            s3.Object(bucketName, path).put(Body= dirName)
                            print("Putting: " + path  + "\n")                  
                    #File
                    else:
                        #check if file has been modified
                         fileStat = os.stat(dirName + "\\" + fname)                    
                         new_metafile.write(path + " " + format(fileStat.st_mtime) + "\n")                                            
                         if (checkForChange(metafile, path, path)):
                            continue
                        #file has been modified, replace the s3 object
                         else:
                            print("Change detected or new file: " + path)
                            s3.Object(bucketName, path).put(Body=open(path, "rb"))
                            print("Putting: " + path + "\n")
                            #test
    
    s3.Object(bucketName, "metadata.txt").put(Body=open("metadata.txt", "rb"))    
    print("Finished backup process at %s" % bucketName)
  
def checkForChange(metafile, path, name):
    #checks if file exists, returns false if it doesnt exist
    if (path in metafile):
        str_index = metafile.find(name)
        time_index = str_index + len(name) + 1
        file_stat = os.stat(path)
        last_modified = format(file_stat.st_mtime)

        #No match found
        if (metafile.find(last_modified, time_index , time_index + len(last_modified) + 1) == -1):
            is_match = False
        #Match found, no need to send
        else:
            is_match = True
        if (is_match):
            return True
        else:
            return False
    else:
        return False

s3 = boto3.resource("s3")
bucketName = getBucketName()
newBucketBool = checkForBucket(bucketName)
if (newBucketBool):	
    backupExistingDirectory(bucketName)
else: 
    backupNewDirectory(bucketName)
try:
    os.remove("metadata.txt")
except:
    print("")