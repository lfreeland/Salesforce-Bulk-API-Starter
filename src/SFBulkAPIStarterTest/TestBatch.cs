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
    public class TestBatch
    {
        [TestMethod]
        public void CreateBatchTest()
        {
            String resultXML =
             @"<?xml version=""1.0"" encoding=""UTF-8""?>
            <batchInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">
            <id>751E0000002YNFFIA4</id>
            <jobId>750E0000001zdZ6IAI</jobId>
            <state>Queued</state>
            <createdDate>2014-09-07T12:13:17.000Z</createdDate>
            <systemModstamp>2014-09-07T12:13:17.000Z</systemModstamp>
            <numberRecordsProcessed>0</numberRecordsProcessed>
            <numberRecordsFailed>0</numberRecordsFailed>
            <totalProcessingTime>0</totalProcessingTime>
            <apiActiveProcessingTime>0</apiActiveProcessingTime>
            <apexProcessingTime>0</apexProcessingTime>
            </batchInfo>";

            Batch batch = Batch.CreateBatch(resultXML);

            Assert.IsTrue(batch != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(batch.Id) == false);
        }
    }
}
