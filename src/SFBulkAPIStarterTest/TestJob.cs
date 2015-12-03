using Microsoft.VisualStudio.TestTools.UnitTesting;
using SFBulkAPIStarter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SFBulkAPIStarterTest
{
    [TestClass]
    public class TestJob
    {
        [TestMethod]
        public void CreateTest()
        {
            String jobXML = @"<?xml version=""1.0"" encoding=""UTF-8""?><jobInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">
                                 <id>750E0000001zcUtIAI</id>
                                 <operation>insert</operation>
                                 <object>Account</object>
                                 <createdById>005E0000000I4DHIA0</createdById>
                                 <createdDate>2014-09-07T00:05:56.000Z</createdDate>
                                 <systemModstamp>2014-09-07T00:05:56.000Z</systemModstamp>
                                 <state>Open</state>
                                 <concurrencyMode>Parallel</concurrencyMode>
                                 <contentType>XML</contentType>
                                 <numberBatchesQueued>0</numberBatchesQueued>
                                 <numberBatchesInProgress>0</numberBatchesInProgress>
                                 <numberBatchesCompleted>0</numberBatchesCompleted>
                                 <numberBatchesFailed>0</numberBatchesFailed>
                                 <numberBatchesTotal>0</numberBatchesTotal>
                                 <numberRecordsProcessed>0</numberRecordsProcessed>
                                 <numberRetries>0</numberRetries>
                                 <apiVersion>31.0</apiVersion>
                                 <numberRecordsFailed>0</numberRecordsFailed>
                                 <totalProcessingTime>0</totalProcessingTime>
                                 <apiActiveProcessingTime>0</apiActiveProcessingTime>
                                 <apexProcessingTime>0</apexProcessingTime>
                                </jobInfo>";

            Job job = Job.Create(jobXML);

            Assert.IsTrue(job != null);

            Assert.AreEqual("750E0000001zcUtIAI", job.Id);


        }
    }
}
