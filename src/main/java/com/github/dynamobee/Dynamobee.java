package com.github.dynamobee;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.github.dynamobee.changeset.ChangeEntry;
import com.github.dynamobee.dao.DynamobeeDao;
import com.github.dynamobee.exception.DynamobeeChangeSetException;
import com.github.dynamobee.exception.DynamobeeConfigurationException;
import com.github.dynamobee.exception.DynamobeeConnectionException;
import com.github.dynamobee.exception.DynamobeeException;
import com.github.dynamobee.utils.ChangeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;


/**
 * Dynamobee runner
 */
public class Dynamobee implements InitializingBean {
	private static final Logger logger = LoggerFactory.getLogger(Dynamobee.class);

	private static final String DEFAULT_CHANGELOG_TABLE_NAME = "dynamobee";
	private static final boolean DEFAULT_WAIT_FOR_LOCK = false;
	private static final long DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME = 5L;
	private static final long DEFAULT_CHANGE_LOG_LOCK_POLL_RATE = 10L;
	private static final boolean DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK = false;

	private DynamobeeDao dao;

	private boolean enabled = true;
	private String changeLogsScanPackage;
	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private DynamoDBTemplate dynamoDBTemplate;
	private DynamoDBMapper dynamoDBMapper;
	private DynamoDBMapperConfig dynamoDBMapperConfig;
	private Environment springEnvironment;


	/**
	 * <p>
	 * Constructor takes com.amazonaws.services.dynamodbv2.AmazonDynamoDB and
	 * com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig object as a parameter.
	 * </p>
	 *
	 * @param amazonDynamoDB database connection client
	 * @param dynamoDBMapperConfig dynamodb config used to override table names
	 * @see AmazonDynamoDB
	 */
	public Dynamobee(AmazonDynamoDB amazonDynamoDB, DynamoDBMapperConfig dynamoDBMapperConfig) {
		this.amazonDynamoDB = amazonDynamoDB;
		if (dynamoDBMapperConfig != null) {
			this.dynamoDBMapperConfig = dynamoDBMapperConfig;
		} else {
			this.dynamoDBMapperConfig = DynamoDBMapperConfig.DEFAULT;
		}
		this.dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB, this.dynamoDBMapperConfig);
		this.dynamoDBTemplate = new DynamoDBTemplate(amazonDynamoDB, this.dynamoDBMapper, this.dynamoDBMapperConfig);
		this.dynamoDB = new DynamoDB(amazonDynamoDB);
		this.dao = new DynamobeeDao(DEFAULT_CHANGELOG_TABLE_NAME, DEFAULT_WAIT_FOR_LOCK,
				DEFAULT_CHANGE_LOG_LOCK_WAIT_TIME, DEFAULT_CHANGE_LOG_LOCK_POLL_RATE, DEFAULT_THROW_EXCEPTION_IF_CANNOT_OBTAIN_LOCK);

		this.setChangelogTableName(DEFAULT_CHANGELOG_TABLE_NAME);
	}

	/**
	 * <p>
	 * Constructor takes com.amazonaws.services.dynamodbv2.AmazonDynamoDB object as a parameter.
	 * </p>
	 * <p>
	 * For more details about <tt>DynamoDB</tt> please see com.amazonaws.services.dynamodbv2.AmazonDynamoDB docs
	 * </p>
	 *
	 * @param amazonDynamoDB database connection client
	 * @see AmazonDynamoDB
	 */
	public Dynamobee(AmazonDynamoDB amazonDynamoDB) {
		this(amazonDynamoDB, null);
	}

	/**
	 * For Spring users: executing dynamobee after bean is created in the Spring context
	 *
	 * @throws Exception exception
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		execute();
	}

	/**
	 * Executing migration
	 *
	 * @throws DynamobeeException exception
	 */
	public void execute() throws DynamobeeException {
		if (!isEnabled()) {
			logger.info("Dynamobee is disabled. Exiting.");
			return;
		}

		validateConfig();

		dao.connectDynamoDB(this.dynamoDB);

		if (!dao.acquireProcessLock()) {
			logger.info("Dynamobee did not acquire process lock. Exiting.");
			return;
		}

		logger.info("Dynamobee acquired process lock, starting the data migration sequence..");

		try {
			executeMigration();
		} catch (Exception e) {
			logger.error("Dynamobee migration failed", e);
			throw e;
		} finally {
			logger.info("Dynamobee is releasing process lock.");
			dao.releaseProcessLock();
		}

		logger.info("Dynamobee has finished his job.");
	}

	private void executeMigration() throws DynamobeeConnectionException, DynamobeeException {

		ChangeService service = new ChangeService(changeLogsScanPackage, springEnvironment);

		for (Class<?> changelogClass : service.fetchChangeLogs()) {

			Object changelogInstance = null;
			try {
				changelogInstance = changelogClass.getConstructor().newInstance();
				List<Method> changesetMethods = service.fetchChangeSets(changelogInstance.getClass());

				for (Method changesetMethod : changesetMethods) {
					ChangeEntry changeEntry = service.createChangeEntry(changesetMethod);

					try {
						if (dao.isNewChange(changeEntry)) {
							executeChangeSetMethod(changesetMethod, changelogInstance, this.dynamoDB);
							dao.save(changeEntry);
							logger.info(changeEntry + " applied");
						} else if (service.isRunAlwaysChangeSet(changesetMethod)) {
							executeChangeSetMethod(changesetMethod, changelogInstance, this.dynamoDB);
							logger.info(changeEntry + " reapplied");
						} else {
							logger.info(changeEntry + " passed over");
						}
					} catch (DynamobeeChangeSetException e) {
						logger.error(e.getMessage());
					}
				}
			} catch (NoSuchMethodException e) {
				throw new DynamobeeException(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				throw new DynamobeeException(e.getMessage(), e);
			} catch (InvocationTargetException e) {
				Throwable targetException = e.getTargetException();
				throw new DynamobeeException(targetException.getMessage(), e);
			} catch (InstantiationException e) {
				throw new DynamobeeException(e.getMessage(), e);
			}

		}
	}

	private Object executeChangeSetMethod(Method changeSetMethod, Object changeLogInstance, DynamoDB db)
			throws IllegalAccessException, InvocationTargetException, DynamobeeChangeSetException {

	  if(changeSetMethod.getParameterTypes().length == 0) {
      return changeSetMethod.invoke(changeLogInstance);
    }

    Object[] args = Stream.of(changeSetMethod.getParameterTypes()).map(parameterType -> {
      if (parameterType.equals(DynamoDB.class)) {
        logger.debug("method with DynamoDB argument");
        return db;
      } else if (parameterType.equals(AmazonDynamoDB.class)) {
        logger.debug("method with AmazonDynamoDB argument");
        return amazonDynamoDB;
      } else if (parameterType.equals(DynamoDBTemplate.class)) {
        logger.debug("method with DynamoDBTemplate argument");
        return dynamoDBTemplate;
      } else if (parameterType.equals(DynamoDBMapper.class)) {
        logger.debug("method with DynamoDBMapper");
        return dynamoDBMapper;
      } else if (parameterType.equals(Environment.class)) {
        logger.debug("method with Environment");
        return springEnvironment;
      } else {
        return null;
      }
    }).toArray();

	  return changeSetMethod.invoke(changeLogInstance, args);
	}

	private void validateConfig() throws DynamobeeConfigurationException {
		if (changeLogsScanPackage == null || changeLogsScanPackage.trim().length() == 0) {
			throw new DynamobeeConfigurationException("Scan package for changelogs is not set: use appropriate setter");
		}
	}

	/**
	 * @return true if an execution is in progress, in any process.
	 * @throws DynamobeeConnectionException exception
	 */
	public boolean isExecutionInProgress() throws DynamobeeConnectionException {
		return dao.isProccessLockHeld();
	}

	/**
	 * Package name where @ChangeLog-annotated classes are kept.
	 *
	 * @param changeLogsScanPackage package where your changelogs are
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setChangeLogsScanPackage(String changeLogsScanPackage) {
		this.changeLogsScanPackage = changeLogsScanPackage;
		return this;
	}

	/**
	 * @return true if Dynamobee runner is enabled and able to run, otherwise false
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Feature which enables/disables Dynamobee runner execution
	 *
	 * @param enabled Dynamobee will run only if this option is set to true
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setEnabled(boolean enabled) {
		this.enabled = enabled;
		return this;
	}

	/**
	 * Feature which enables/disables waiting for lock if it's already obtained
	 *
	 * @param waitForLock Dynamobee will be waiting for lock if it's already obtained if this option is set to true
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setWaitForLock(boolean waitForLock) {
		this.dao.setWaitForLock(waitForLock);
		return this;
	}

	/**
	 * Waiting time for acquiring lock if waitForLock is true
	 *
	 * @param changeLogLockWaitTime Waiting time in minutes for acquiring lock
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setChangeLogLockWaitTime(long changeLogLockWaitTime) {
		this.dao.setChangeLogLockWaitTime(changeLogLockWaitTime);
		return this;
	}

	/**
	 * Poll rate for acquiring lock if waitForLock is true
	 *
	 * @param changeLogLockPollRate Poll rate in seconds for acquiring lock
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setChangeLogLockPollRate(long changeLogLockPollRate) {
		this.dao.setChangeLogLockPollRate(changeLogLockPollRate);
		return this;
	}

	/**
	 * Feature which enables/disables throwing DynamobeeLockException if Dynamobee can not obtain lock
	 *
	 * @param throwExceptionIfCannotObtainLock Dynamobee will throw DynamobeeLockException if lock can not be obtained
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
		this.dao.setThrowExceptionIfCannotObtainLock(throwExceptionIfCannotObtainLock);
		return this;
	}

	/**
	 * Set Environment object for Spring Profiles (@Profile) integration
	 *
	 * @param environment org.springframework.core.env.Environment object to inject
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setSpringEnvironment(Environment environment) {
		this.springEnvironment = environment;
		return this;
	}

	/**
	 * Sets pre-configured {@link DynamoDBTemplate} instance to use by the Dynamobee
	 *
	 * @param dynamoDBTemplate instance of the {@link DynamoDBTemplate}
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setDynamoDBTemplate(DynamoDBTemplate dynamoDBTemplate) {
		this.dynamoDBTemplate = dynamoDBTemplate;
		return this;
	}

	/**
	 * Overwrites a default dynamobee changelog collection hardcoded in DEFAULT_CHANGELOG_TABLE_NAME.
	 *
	 * CAUTION! Use this method carefully - when changing the name on a existing system,
	 * your changelogs will be executed again on your DynamoDB instance
	 *
	 * @param changelogTableName a new changelog collection name
	 * @return Dynamobee object for fluent interface
	 */
	public Dynamobee setChangelogTableName(String changelogTableName) {

		if (dynamoDBMapperConfig != null && dynamoDBMapperConfig.getTableNameOverride() != null
				&& dynamoDBMapperConfig.getTableNameOverride().getTableNamePrefix() != null) {
			changelogTableName = dynamoDBMapperConfig.getTableNameOverride().getTableNamePrefix() + changelogTableName;
		}

		this.dao.setChangelogTableName(changelogTableName);

		return this;
	}
}
