from hdfs import Config
from Base import ParameterizedCommand
from hdfs.ext.kerberos import KerberosClient
from Framework.Environment import get_Global_Configuration, get_Security_Context
from Framework.Configuration import get_Cluster
from datetime import timedelta

import os 
import hashlib

class Command(ParameterizedCommand): 
    def __init__(self):
        hash_list=[]
        self.cluster = str(get_Cluster().name)
        pass
     
    def hash_method(self,file_name):        
        match=False               
        m=hashlib.md5(file_name.encode())
        for hashObj in self.hash_list:
            if (hashObj==m):
                match=True
                break
        if (match==False):
            self.hash_list.add(m)
        return match
     
    def create_hash_list(self, target_path):
        target_list=os.listdir(target_path)
        for target_file in target_list:
            m=hashlib.md5(target_file.enconde)
            self.hash_list.add(m)
                    
    def main(self, *args, **options):
        source_path=options['source']
        target_path=options['target']
        source_list=os.listdir(source_path)
        
        for file in source_list: 
            file_prefix=options['prefix']
            found=file.find(file_prefix)
            if (found != -1):
                #checking the source directory
                try:
                    os.path.exists(target_path):
                    create_hash_list(self, target_path)
                    
                except:
                    os.makedirs(target_path)
               
                already_copied=self.hash_method(file)
                if(already_copied==True):
                    continue;
                mod_time = os.stat(file).st_MTIME
                new_filename=mod_time+file
                file.rename(new_filename)
                self.copy_to_hdfs(self,source_path,target_path)
    
    def copy_to_hdfs(self,source,target):
        # Get the client for HdfsCLI
        conf = get_Global_Configuration()
        url = conf.get_Configuration('httpfs.url')
        client = KerberosClient(url=url)

        # Execute as HDFS user
        with get_Security_Context('hdfs'):
            # Get FileStatus of desired dir/file.
            try:
                upload_path = client.upload(source,target)
            except:
                print('Could not copy over access log')
        
                       
                    
                
           
       
        
        
        
        
        
          

        
    
    