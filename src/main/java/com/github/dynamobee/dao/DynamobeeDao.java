package com.github.dynamobee.dao;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.github.dynamobee.exception.DynamobeeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dynamobee.changeset.ChangeEntry;
import com.github.dynamobee.exception.DynamobeeConfigurationException;
import com.github.dynamobee.exception.DynamobeeConnectionException;
import com.github.dynamobee.exception.DynamobeeLockException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;


public class DynamobeeDao {
	private static final Logger logger = LoggerFactory.getLogger("Dynamobee dao");

	private static final String VALUE_LOCK = "LOCK";

	private DynamoDbClient dynamoDbClient;
	private String dynamobeeTableName;
	private TableDescription dynamobeeTable;
	private boolean waitForLock;
	private long changeLogLockWaitTime;
	private long changeLogLockPollRate;
	private boolean throwExceptionIfCannotObtainLock;
	private String partitionKey;

	public DynamobeeDao(String dynamobeeTableName, boolean waitForLock, long changeLogLockWaitTime,
			long changeLogLockPollRate, boolean throwExceptionIfCannotObtainLock) {
		this.dynamobeeTableName = dynamobeeTableName;
		this.waitForLock = waitForLock;
		this.changeLogLockWaitTime = changeLogLockWaitTime;
		this.changeLogLockPollRate = changeLogLockPollRate;
		this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
	}

  public DynamobeeDao(String dynamobeeTableName, boolean waitForLock, long changeLogLockWaitTime,
                      long changeLogLockPollRate, boolean throwExceptionIfCannotObtainLock, String partitionKey) {
    this.dynamobeeTableName = dynamobeeTableName;
    this.waitForLock = waitForLock;
    this.changeLogLockWaitTime = changeLogLockWaitTime;
    this.changeLogLockPollRate = changeLogLockPollRate;
    this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
    this.partitionKey = partitionKey;
  }

	public void connectDynamoDB(DynamoDbClient dynamoDB) throws DynamobeeException {
		this.dynamoDbClient = dynamoDB;
		this.dynamobeeTable = findDynamoBeeTable();
	}

	private TableDescription findDynamoBeeTable() throws DynamobeeException {
		logger.info("Searching for an existing DynamoBee table; please wait...");
		try {
			TableDescription tableDescription = dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(dynamobeeTableName).build()).table();
			logger.info("DynamoBee table found");
			return tableDescription;

		} catch (ResourceNotFoundException e) {
			throw new DynamobeeException("Could not find the specified migrations table!");
		}
	}

	/**
	 * Try to acquire process lock
	 *
	 * @return true if successfully acquired, false otherwise
	 * @throws DynamobeeConnectionException exception
	 * @throws DynamobeeLockException exception
	 */
	public boolean acquireProcessLock() throws DynamobeeConnectionException, DynamobeeLockException {
		boolean acquired = this.acquireLock();

		if (!acquired && waitForLock) {
			long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
			while (!acquired && new Date().getTime() < timeToGiveUp) {
				acquired = this.acquireLock();
				if (!acquired) {
					logger.info("Waiting for changelog lock....");
					try {
						Thread.sleep(changeLogLockPollRate * 1000);
					} catch (InterruptedException e) {
						// nothing
					}
				}
			}
		}

		if (!acquired && throwExceptionIfCannotObtainLock) {
			logger.info("Dynamobee did not acquire process lock. Throwing exception.");
			throw new DynamobeeLockException("Could not acquire process lock");
		}

		return acquired;
	}

	public boolean acquireLock() {
		// acquire lock by attempting to insert the same value in the collection - if it already exists (i.e. lock held)
		// there will be an exception
		try {
			HashMap<String, AttributeValue> item = new HashMap();
			item.put(ChangeEntry.KEY_CHANGEID, AttributeValue.builder().s(VALUE_LOCK).build());
			item.put(ChangeEntry.KEY_TIMESTAMP, AttributeValue.builder().n(Long.toString(new Date().getTime())).build());
			item.put(ChangeEntry.KEY_AUTHOR, AttributeValue.builder().s(getHostName()).build());

			PutItemRequest request = PutItemRequest
          .builder()
          .item(item)
          .conditionExpression("attribute_not_exists(" + ChangeEntry.KEY_CHANGEID + ")")
          .tableName(dynamobeeTableName)
          .build();

			this.dynamoDbClient.putItem(request);
		} catch (ConditionalCheckFailedException ex) {
			logger.warn("The lock has been already acquired.");
			return false;
		}
		return true;
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "UnknownHost";
		}
	}

	public void releaseProcessLock() throws DynamobeeConnectionException {
	  Map<String, AttributeValue> deleteKey = new HashMap();
	  deleteKey.put(ChangeEntry.KEY_CHANGEID, AttributeValue.builder().s(VALUE_LOCK).build());

		this.dynamoDbClient.deleteItem(
		    DeleteItemRequest
            .builder()
            .tableName(dynamobeeTableName)
            .key(deleteKey)
            .build());
	}

	public boolean isProccessLockHeld() throws DynamobeeConnectionException {
    Map<String, AttributeValue> getKey = new HashMap();
    getKey.put(ChangeEntry.KEY_CHANGEID, AttributeValue.builder().s(VALUE_LOCK).build());

		return this.dynamoDbClient.getItem(
		    GetItemRequest
            .builder()
            .tableName(dynamobeeTableName)
            .key(getKey)
            .consistentRead(true)
            .build()
    ).hasItem();
	}

	public boolean isNewChange(ChangeEntry changeEntry) throws DynamobeeConnectionException {
    Map<String, AttributeValue> getKey = new HashMap();
    getKey.put(ChangeEntry.KEY_CHANGEID, AttributeValue.builder().s(changeEntry.getChangeId()).build());

    return !this.dynamoDbClient.getItem(
        GetItemRequest
            .builder()
            .tableName(dynamobeeTableName)
            .key(getKey)
            .consistentRead(true)
            .build()
    ).hasItem();
	}

	public void save(ChangeEntry changeEntry) throws DynamobeeConnectionException {
    PutItemRequest request = PutItemRequest
        .builder()
        .item(changeEntry.buildFullDBObject())
        .conditionExpression("attribute_not_exists(" + ChangeEntry.KEY_CHANGEID + ")")
        .tableName(dynamobeeTableName)
        .build();
    this.dynamoDbClient.putItem(request);
	}

	public void setChangelogTableName(String changelogCollectionName) {
		this.dynamobeeTableName = changelogCollectionName;
	}

	public boolean isWaitForLock() {
		return waitForLock;
	}

	public void setWaitForLock(boolean waitForLock) {
		this.waitForLock = waitForLock;
	}

	public long getChangeLogLockWaitTime() {
		return changeLogLockWaitTime;
	}

	public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
		this.changeLogLockWaitTime = changeLogLockWaitTime;
	}

	public long getChangeLogLockPollRate() {
		return changeLogLockPollRate;
	}

	public void setChangeLogLockPollRate(long changeLogLockPollRate) {
		this.changeLogLockPollRate = changeLogLockPollRate;
	}

	public boolean isThrowExceptionIfCannotObtainLock() {
		return throwExceptionIfCannotObtainLock;
	}

	public void setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
		this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
	}

}
