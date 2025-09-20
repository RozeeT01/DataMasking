# DataMasking
Mask the data in below format.  
# data is:
(Renuka1992@gmail.com", "9856765434"), ("anbuarasu@hotmail.com", "9844567788")], ["email", "mobile"])  
<br/>
<p align="center">
Input: <br/>
<img src="https://github.com/user-attachments/assets/2cf8fcf1-12f5-4316-bfab-2383bf2ff8ad" height="40%" width="40%" alt="Disk Sanitization Steps"/>
<br />
<p align="center">
Output: <br/>
 <img src="https://github.com/user-attachments/assets/f750d4bf-ef56-43ea-9565-97648d903892" height="40%" width="40%" alt="Disk Sanitization Steps"/>   
  <br/>
<h2>Code used to solve this:</h2>
df = spark.createDataFrame([("Renuka1992@gmail.com", "9856765434"), ("anbuarasu@hotmail.com", "9844567788")], ["email", "mobile"])  
df.show()  


splitdf =(  

    df.withColumn("user",expr("split(email,'@')[0]"))  
    .withColumn("emailc",expr("split(email,'@')[1]"))  
    .withColumn("usersize", expr("length(user)"))  
    .withColumn("firstchar",expr("substring(user,0,1)"))  
    .withColumn("lastchar",expr("substring(user,-2,2)"))  
    .withColumn("maskedemail",expr("concat(firstchar, repeat('*',usersize -3)   , lastchar , '@' , emailc)"))  
)  

splitdf.show()  

mobiledf = (  
    splitdf.withColumn("mobilesize", expr("length(mobile)"))  
        .withColumn("firstchar", expr("substring(mobile,0,2)"))  
        .withColumn("lastchar",expr("substring(mobile,-3,3)"))  
        .withColumn("maskedmobile", expr("concat(firstchar, repeat('*',mobilesize -5), lastchar)"))  

)  
mobiledf.show()    

finaldf = (mobiledf  
           .select("maskedemail","maskedmobile")  
           .withColumnRenamed("maskedemail", "email")  
           .withColumnRenamed("maskedmobile", "mobile")  
           )  
finaldf.show()  
# Walking thru the code:   
First change the data into proper dataframe.


