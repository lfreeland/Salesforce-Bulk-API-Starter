using System;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SFBulkAPIStarter;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SFBulkAPIStarterTest
{
    [TestClass]
    public class TestBulkApiClient
    {
        SFBulkAPIStarter.BulkApiClient _apiClient = null;

        [TestInitialize]
        public void Setup()
        {
            String username = ConfigurationManager.AppSettings["Username"];
            String password = ConfigurationManager.AppSettings["Password"];
            String loginUrl = ConfigurationManager.AppSettings["LoginUrl"];
            String securityToken = ConfigurationManager.AppSettings["SecurityToken"];

            _apiClient = new SFBulkAPIStarter.BulkApiClient(username, password + securityToken, loginUrl);
        }

        [TestMethod]
        public void CreateAccountJobTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();

            Job job = _apiClient.CreateJob(jobRequest);

            Assert.IsTrue(job != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(job.Id) == false);
            Assert.AreEqual("Open", job.State);

            Job closedJob = _apiClient.CloseJob(job.Id);

            Assert.AreEqual("Closed", closedJob.State);
        }

        [TestMethod]
        public void GetAccountJobTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();

            Job job = _apiClient.CreateJob(jobRequest);
            Job closedJob = _apiClient.CloseJob(job.Id);

            closedJob = _apiClient.GetJob(closedJob.Id);

            Assert.IsTrue(closedJob != null);
            Assert.AreEqual("Closed", closedJob.State);
        }
        [TestMethod]
        public void InsertAccountsWith1BatchTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            Assert.IsTrue(accountBatch != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetJob(job.Id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.Id);
            }

            Assert.IsTrue(job.NumberRecordsFailed == 0);
            Assert.IsTrue(job.NumberRecordsProcessed == 1);
        }

        [TestMethod]
        public void GetBatchTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.JobId) == false);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            Batch batch = _apiClient.GetBatch(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(accountBatch.JobId, batch.JobId);
            Assert.AreEqual(accountBatch.Id, batch.Id);
        }

        [TestMethod]
        public void GetBatchesTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchContents2 = "Name" + Environment.NewLine;
            String accountName2 = "Test Name2";
            batchContents2 += accountName2;

            CreateBatchRequest batchRequest2 = buildCreateBatchRequest(job.Id, batchContents2);

            Batch accountBatch2 = _apiClient.CreateBatch(batchRequest2);

            List<Batch> batches = _apiClient.GetBatches(job.Id);

            Assert.AreEqual(2, batches.Count);
        }

        [TestMethod]
        public void GetBatchRequestTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchRequestContents = _apiClient.GetBatchRequest(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(batchContents, batchRequestContents);
        }

        [TestMethod]
        public void GetBatchResultsTest()
        {
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchResults = _apiClient.GetBatchResults(accountBatch.JobId, accountBatch.Id);

            Assert.IsTrue(String.IsNullOrWhiteSpace(batchResults) == false);
        }

        [TestMethod]
        public void QueryAccountTest()
        {
            // Insert an account so there's at least one to query

            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetJob(job.Id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.Id);
            }

            CreateJobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id, Name FROM Account WHERE Name = '" + accountName + "'";

            CreateBatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.Id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            _apiClient.CloseJob(queryJob.Id);

            queryJob = _apiClient.GetJob(queryJob.Id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.Id);
            }

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);

            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);

            Assert.AreEqual(1, resultIds.Count);

            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);

            Assert.IsTrue(batchQueryResults.Contains(accountName));
        }

        [TestMethod]
        public void UpsertAccountTest()
        {
            // Upsert an account
            String externalFieldName = ConfigurationManager.AppSettings["ExternalFieldName"];
            CreateJobRequest jobRequest = buildDefaultUpsertAccountCreateJobRequest(externalFieldName);

            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetJob(job.Id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.Id);
            }

            Assert.IsTrue(job.NumberBatchesFailed == 0);
            Assert.IsTrue(job.NumberRecordsProcessed > 0);
        }

        [TestMethod]
        public void DeleteAccountTest()
        {
            // Insert an account so there's at least one to delete

            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetJob(job.Id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.Id);
            }

            // Query the accounts to dynamically retreive the account id to delete
            CreateJobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id FROM Account WHERE Name = '" + accountName + "'";

            CreateBatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.Id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            _apiClient.CloseJob(queryJob.Id);

            queryJob = _apiClient.GetJob(queryJob.Id);

            while (queryJob.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.Id);
            }

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);

            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);

            Assert.AreEqual(1, resultIds.Count);

            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);

            String[] batchQueryResultsParts = batchQueryResults.Split(new String[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

            String firstAccountIdToDelete = batchQueryResultsParts[1].Replace(@"""", String.Empty);


            // Delete the account
            CreateJobRequest deleteAccountJobRequest = buildDefaultDeleteAccountCreateJobRequest();
            Job deleteJob = _apiClient.CreateJob(deleteAccountJobRequest);

            String deleteBatchContents = "Id" + Environment.NewLine + firstAccountIdToDelete;

            CreateBatchRequest deleteBatchRequest = buildCreateBatchRequest(deleteJob.Id, deleteBatchContents);
            Batch deleteBatch = _apiClient.CreateBatch(deleteBatchRequest);

            _apiClient.CloseJob(deleteJob.Id);

            deleteJob = _apiClient.GetJob(deleteJob.Id);

            while (deleteJob.IsDone == false)
            {
                Thread.Sleep(2000);
                deleteJob = _apiClient.GetJob(deleteJob.Id);
            }

            Assert.AreEqual(1, deleteJob.NumberRecordsProcessed);
        }

        private CreateJobRequest buildDefaultDeleteAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Delete);
        }

        private CreateJobRequest buildDefaultQueryAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Query);
        }

        private CreateJobRequest buildDefaultUpsertAccountCreateJobRequest(String externalIdFieldName)
        {
            CreateJobRequest request = buildDefaultAccountCreateJobRequest(JobOperation.Upsert);

            request.ExternalIdFieldName = externalIdFieldName;

            return request;
        }

        private CreateJobRequest buildDefaultInsertAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Insert);
        }

        private CreateJobRequest buildDefaultAccountCreateJobRequest(JobOperation operation)
        {
            CreateJobRequest jobRequest = new CreateJobRequest();
            jobRequest.ContentType = JobContentType.CSV;
            jobRequest.Operation = operation;
            jobRequest.Object = "Account";

            return jobRequest;
        }

        private CreateBatchRequest buildCreateBatchRequest(String jobId, String batchContents)
        {
            return new CreateBatchRequest()
            {
                JobId = jobId,
                BatchContents = batchContents
            };
        }
    }
}
