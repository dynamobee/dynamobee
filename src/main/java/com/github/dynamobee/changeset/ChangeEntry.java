package com.github.dynamobee.changeset;

import java.util.Date;
import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import org.w3c.dom.Attr;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;


/**
 * Entry in the changes collection log {@link com.github.dynamobee.Dynamobee#DEFAULT_CHANGELOG_TABLE_NAME}
 * Type: entity class.
 */
public class ChangeEntry {
	public static final String KEY_CHANGEID = "changeId";
	public static final String KEY_AUTHOR = "author";
	public static final String KEY_TIMESTAMP = "timestamp";
	public static final String KEY_CHANGELOGCLASS = "changeLogClass";
	public static final String KEY_CHANGESETMETHOD = "changeSetMethod";

	private String changeId;
	private String author;
	private Date timestamp;
	private String changeLogClass;
	private String changeSetMethodName;

	public ChangeEntry(String changeId, String author, Date timestamp, String changeLogClass, String changeSetMethodName) {
		this.changeId = changeId;
		this.author = author;
		this.timestamp = new Date(timestamp.getTime());
		this.changeLogClass = changeLogClass;
		this.changeSetMethodName = changeSetMethodName;
	}

	public HashMap<String, AttributeValue> buildFullDBObject() {
    HashMap<String, AttributeValue> item = new HashMap();
    item.put(KEY_CHANGEID, AttributeValue.builder().s(this.changeId).build());
    item.put(KEY_AUTHOR, AttributeValue.builder().s(this.author).build());
    item.put(KEY_TIMESTAMP, AttributeValue.builder().s(Long.toString(this.timestamp.getTime())).build());
    item.put(KEY_CHANGELOGCLASS, AttributeValue.builder().s(this.changeLogClass).build());
		item.put(KEY_CHANGESETMETHOD, AttributeValue.builder().s(this.changeSetMethodName).build());
		return item;
	}

	public QuerySpec buildSearchQuerySpec() {
//		return new Document()
//				.append(KEY_CHANGEID, this.changeId)
//				.append(KEY_AUTHOR, this.author);
		return new QuerySpec().withKeyConditionExpression(KEY_CHANGEID + " = " + this.changeId);
	}

	@Override
	public String toString() {
		return "[ChangeSet: id=" + this.changeId +
				", author=" + this.author +
				", changeLogClass=" + this.changeLogClass +
				", changeSetMethod=" + this.changeSetMethodName + "]";
	}

	public String getChangeId() {
		return this.changeId;
	}

	public String getAuthor() {
		return this.author;
	}

	public Date getTimestamp() {
		return this.timestamp;
	}

	public String getChangeLogClass() {
		return this.changeLogClass;
	}

	public String getChangeSetMethodName() {
		return this.changeSetMethodName;
	}

}
