using System;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SFBulkAPIStarter;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;

namespace SFBulkAPIStarterTest
{
    /// <summary>
    /// This class tests the BulkApiClient class to ensure the Salesforce Bulk API
    /// is invoked correctly.
    /// </summary>
    [TestClass]
    public class TestBulkApiClient
    {
        private static readonly String DEFAULT_ACCOUNT_NAME = "Test Name";

        SFBulkAPIStarter.BulkApiClient _apiClient = null;

        /// <summary>
        /// Initialize the Bulk Api Client and login to the org using the
        /// Username, Password, LoginUrl, and SecurityToken app settings.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            String username = ConfigurationManager.AppSettings["Username"];
            String password = ConfigurationManager.AppSettings["Password"];
            String loginUrl = ConfigurationManager.AppSettings["LoginUrl"];
            String securityToken = ConfigurationManager.AppSettings["SecurityToken"];

            _apiClient = new SFBulkAPIStarter.BulkApiClient(username, password + securityToken, loginUrl);
        }

        /// <summary>
        /// Test creating and closing a job for inserting accounts without any batches.
        /// </summary>
        [TestMethod]
        public void CreateAccountJobTest()
        {
            // Create insert account job
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            Assert.IsTrue(insertAccountJob != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(insertAccountJob.Id) == false);
            Assert.AreEqual("Open", insertAccountJob.State);

            // Close job so no more batches are added.
            Job closedJob = _apiClient.CloseJob(insertAccountJob.Id);

            Assert.AreEqual("Closed", closedJob.State);
        }

        /// <summary>
        /// Tests getting a job after creating one. 
        /// </summary>
        [TestMethod]
        public void GetAccountJobTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();

            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            // Close job so no more batches are added.
            Job closedInsertAccountJob = _apiClient.CloseJob(insertAccountJob.Id);

            closedInsertAccountJob = _apiClient.GetJob(closedInsertAccountJob.Id);

            Assert.IsTrue(closedInsertAccountJob != null);
            Assert.AreEqual("Closed", closedInsertAccountJob.State);
        }

        /// <summary>
        /// Tests creating an insert account job with one batch with one account in it.
        /// </summary>
        [TestMethod]
        public void InsertAccountsWith1BatchTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest insertAccountBatchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(insertAccountBatchRequest);

            Assert.IsTrue(accountBatch != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            // Close job so no more batches are added.
            _apiClient.CloseJob(insertAccountJob.Id);

            insertAccountJob = _apiClient.GetCompletedJob(insertAccountJob.Id);

            Assert.IsTrue(insertAccountJob.NumberRecordsFailed == 0);
            Assert.IsTrue(insertAccountJob.NumberRecordsProcessed == 1);
        }

        /// <summary>
        /// Tests getting a batch's detail after an account batch is created.
        /// </summary>
        [TestMethod]
        public void GetBatchTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.JobId) == false);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            Batch batch = _apiClient.GetBatch(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(accountBatch.JobId, batch.JobId);
            Assert.AreEqual(accountBatch.Id, batch.Id);
        }

        /// <summary>
        /// Tests getting the batches in a job.
        /// </summary>
        [TestMethod]
        public void GetJobBatchesTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchContents2 = buildDefaultAccountBatchContents("Test Name2");

            CreateBatchRequest batchRequest2 = buildCreateBatchRequest(insertAccountJob.Id, batchContents2);

            Batch accountBatch2 = _apiClient.CreateBatch(batchRequest2);

            List<Batch> batches = _apiClient.GetBatches(insertAccountJob.Id);

            Assert.AreEqual(2, batches.Count);
        }

        /// <summary>
        /// Tests getting the batch contents for a specific batch in a specific job.
        /// </summary>
        [TestMethod]
        public void GetBatchRequestTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchRequestContents = _apiClient.GetBatchRequest(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(batchContents, batchRequestContents);
        }

        /// <summary>
        /// Tests getting a batch's results file for a specified batch on a given job.
        /// </summary>
        [TestMethod]
        public void GetBatchResultsTest()
        {
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchResults = _apiClient.GetBatchResults(accountBatch.JobId, accountBatch.Id);

            Assert.IsTrue(String.IsNullOrWhiteSpace(batchResults) == false);
        }

        /// <summary>
        /// Tests querying an account after an account is inserted.
        /// </summary>
        [TestMethod]
        public void QueryAccountTest()
        {
            // Insert an account so there's at least one to query
            CreateJobRequest insertAccountJobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job insertAccountJob = _apiClient.CreateJob(insertAccountJobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(insertAccountJob.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(insertAccountJob.Id);

            insertAccountJob = _apiClient.GetCompletedJob(insertAccountJob.Id);

            // Query the account that was inserted.
            CreateJobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id, Name FROM Account WHERE Name = '" + DEFAULT_ACCOUNT_NAME + "'";

            CreateBatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.Id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            _apiClient.CloseJob(queryJob.Id);

            queryJob = _apiClient.GetCompletedJob(queryJob.Id);

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);

            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);

            Assert.AreEqual(1, resultIds.Count);

            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);

            Assert.IsTrue(batchQueryResults.Contains(DEFAULT_ACCOUNT_NAME));
        }

        /// <summary>
        /// Tests upserting an account using the specified ExternalFieldName app setting.
        /// </summary>
        [TestMethod]
        public void UpsertAccountTest()
        {
            // Upsert an account
            String externalFieldName = ConfigurationManager.AppSettings["ExternalFieldName"];

            Assert.AreNotEqual("external_id_here_for_upsert_test", externalFieldName, "Please specify an external id account field.");

            CreateJobRequest jobRequest = buildDefaultUpsertAccountCreateJobRequest(externalFieldName);

            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            // Close job so no more batches are added.
            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetCompletedJob(job.Id);

            Assert.IsTrue(job.NumberBatchesFailed == 0);
            Assert.IsTrue(job.NumberRecordsProcessed > 0);
        }

        /// <summary>
        /// Tests deleting an account record that is initially inserted.
        /// </summary>
        [TestMethod]
        public void DeleteAccountTest()
        {
            // Insert an account so there's at least one to delete
            CreateJobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = buildDefaultAccountBatchContents();

            CreateBatchRequest batchRequest = buildCreateBatchRequest(job.Id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.Id);

            job = _apiClient.GetCompletedJob(job.Id);

            // Query the accounts to dynamically retreive the account id to delete
            CreateJobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id FROM Account WHERE Name = '" + DEFAULT_ACCOUNT_NAME + "'";

            CreateBatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.Id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            // Close job so no more batches are added.
            _apiClient.CloseJob(queryJob.Id);

            queryJob = _apiClient.GetCompletedJob(queryJob.Id);

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

            // Close job so no more batches are added.
            _apiClient.CloseJob(deleteJob.Id);

            deleteJob = _apiClient.GetCompletedJob(deleteJob.Id);

            Assert.AreEqual(1, deleteJob.NumberRecordsProcessed);
        }

        /// <summary>
        /// Tests attaching a file to the specified "AttachmentParentId" record.
        /// </summary>
        [TestMethod]
        public void AttachFileTest()
        {
            String parentId = ConfigurationManager.AppSettings["AttachmentParentId"];

            Assert.IsTrue(String.IsNullOrWhiteSpace(parentId) == false, "The AttachmentParentId app setting is blank. Please specify the id of a salesforce record to attach an attachment file.");

            // Create Attachment Job
            CreateJobRequest attachmentJobRequest = buildDefaultAttachmentJobRequest();
            Job attachmentJob = _apiClient.CreateJob(attachmentJobRequest);

            // Create file to attach to record
            String filename = "BulkAPIClientAttachment.txt";
            File.WriteAllText(filename, "Hello From Attach File Test");

            // Create attachment batch request
            CreateAttachmentBatchRequest attachmentBatchRequest = new CreateAttachmentBatchRequest();
            attachmentBatchRequest.FilePath = filename;
            attachmentBatchRequest.JobId = attachmentJob.Id;
            attachmentBatchRequest.ParentId = parentId;

            // Create attachment batch
            Batch attachmentBatch = _apiClient.CreateAttachmentBatch(attachmentBatchRequest);

            // Close job so no more batches are added.
            _apiClient.CloseJob(attachmentJob.Id);

            attachmentJob = _apiClient.GetCompletedJob(attachmentJob.Id);

            Assert.AreEqual(1, attachmentJob.NumberRecordsProcessed, "The file was not attached to the specified record.");
        }

        private string buildDefaultAccountBatchContents()
        {
            return buildDefaultAccountBatchContents(DEFAULT_ACCOUNT_NAME);
        }

        private string buildDefaultAccountBatchContents(String accountName)
        {
            String batchContents = "Name" + Environment.NewLine;
            batchContents += accountName;

            return batchContents;
        }

        private CreateJobRequest buildDefaultAttachmentJobRequest()
        {
            CreateJobRequest jobRequest = new CreateJobRequest();

            jobRequest.ContentType = JobContentType.ZIP_CSV;
            jobRequest.Operation = JobOperation.Insert;
            jobRequest.Object = "Attachment";

            return jobRequest;
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
