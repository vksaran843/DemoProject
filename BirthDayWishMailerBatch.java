package com.tbsl.batch.tj.jobAlert.itemReader;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;

import com.tbs.search.impl.RemoteSearchResults;
import com.tbsl.batch.domain.GenericObject;
import com.tbsl.batch.domain.TypeDetails;
import com.tbsl.batch.tj.jobAlert.CandidateSearchCritera;
import com.tbsl.batch.tj.jobAlert.JOBAlertConstants;
import com.tbsl.batch.tj.jobAlert.itemWriter.MailContentProvider;
import com.tbsl.batch.tj.jobAlert.itemWriter.Sender;
import com.tbsl.batch.util.CandidateSearcher;
import com.tbsl.batch.util.CommonSqlQueries;
import com.tbsl.batch.util.EncDecUtil;
import com.tbsl.batch.util.JDBCUtils;

/**
 * 
 * @author AlokKumarPandey
 *
 */
public class BirthDayWishMailerBatch extends StepExecutionListenerSupport implements Tasklet  {

	public static final Log logger = LogFactory.getLog(BirthDayWishMailerBatch.class);
	
	private static final String FROM_ADDRESS = JOBAlertConstants.ACTIVATION_MAILER_ADDRESS;

	private static final String SUBJECT_FOR_BIRTHDAY_WISH_MAILER = "Hi <FNAME>, On Your Birthday, TimesJobs Wishes for Your Successful Career in the Years to Come";
	
	//private static final String BIRTHDAY_WISH_UTM = 
			//"utm_source=birthday_mailer_TJ&utm_medium=email&utm_campaign=birthday_mailer_TJ_cand";
			String BIRTHDAY_WISH_UTM = "utm_source=birthday_mailer_TJ&utm_medium=email&utm_campaign=birthday_mailer_TJ_"+
					JOBAlertConstants.BIRTHDAY_WISH_MAILER_COMBINATION_ID + "_" +
					JOBAlertConstants.BIRTHDAY_WISH_MAILER_TEMPLATE_ID + 
					"_<action>&siteparams=150p_birthday_mailer";

	private static final String URL_PARAM_SEPARTAOR = "?";
	private static final String BIRTHDAY_WISH_PAGE_URL = "/candidate/birthdayHoroscope.html";
	private static final String TJ_LOGO_URL = "/candidate/myhome.html";
	private static final String PAGE_NO_KEY = "PageNo";
	private static final String BIRTHDAY_WISH_TEMPLATE_NAME = "birthDayWishTemplate.vm";

	private static Date currentDate = new Date();
	private static SimpleDateFormat dateFormat;
	private static List<TypeDetails> typeDetail;

	private JDBCUtils jdbcUtils;
	private CandidateSearcher searcher;
	private DataFieldMaxValueIncrementer tracker;
	private Sender sender;
	private MailContentProvider contentProvider;
	private int minAgeOfCandidate;
	private int maxAgeOfCandidate;
	private int pageSize;
	private int maxNoOfCandidatesToProcess;
	private CandidateSearchCritera candidateSearchCritera;
	private int lastRunID;
	private int pageNoToProcess;
	
	@Override
	public void beforeStep(StepExecution stepExecution) {
		logger.error("Entering beforeStep() of BirthDayWishMailerBatch");
		//Create the candidate search criteria
		dateFormat = new SimpleDateFormat("yyyyMMdd");
		StringBuffer dobValues = new StringBuffer();
		for (int presentAge = minAgeOfCandidate; presentAge <= maxAgeOfCandidate; presentAge++) {
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.YEAR, -presentAge);
			dobValues.append(" " + dateFormat.format(calendar.getTime()));
		}
		//candidateSearchCritera = new CandidateSearchCritera();
		candidateSearchCritera.setDob(dobValues.toString());
		//candidateSearchCritera.setNetStatus("11");
		//fetch the last (most recent) run id of process that run on today
		dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		lastRunID = jdbcUtils.getLastRunIDByJobId( "BirthDayWish" + " " + dateFormat.format(currentDate), null);
		//find the no of page processed in that run
		int lastProcessedPageNo = lastRunID == 0 ? -1 : jdbcUtils.getValueFromNameForLastRun(
				"BirthDayWish" + " " + dateFormat.format(currentDate), lastRunID, PAGE_NO_KEY);
		pageNoToProcess = lastProcessedPageNo;
		//Initialise and assign the type detail
		typeDetail = new ArrayList<TypeDetails>();
		typeDetail.add(new TypeDetails("Attribute1", "FIRST_NAME", "String"));
		typeDetail.add(new TypeDetails("Attribute2", "EMAIL2", "String"));
		
		logger.error("BirthDay wishes are being sent on: " +  dateFormat.format(currentDate));
		logger.error("Exiting beforeStep() of BirthDayWishMailerBatch");
	}
	
	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		int mailsSentCount = 0;
		int mailsFailedtCount = 0;
		//for first page that is to be processed today, there should be checked for each record 
		//if mail was sent previously to it, as it might be possible that few mails would have been sent in last run 
		//and process aborted before it could update that page had been processed
		boolean mailSentCheckRequired = true;
		RemoteSearchResults searchResults;
		List<GenericObject> candidates;
		GenericObject candidate;
		String encryptedHoroscopeUrl = EncDecUtil.encrypt(
				MailContentProvider.BASE_URL + BIRTHDAY_WISH_PAGE_URL + URL_PARAM_SEPARTAOR + BIRTHDAY_WISH_UTM.replaceAll("<action>", "Stars_Say"));
		String encryptedTjLogoUrl = EncDecUtil.encrypt(
				MailContentProvider.BASE_URL + TJ_LOGO_URL + URL_PARAM_SEPARTAOR + BIRTHDAY_WISH_UTM.replaceAll("<action>", "TJ_Logo"));
		//Proceed only if on next page fetch, total candidates will not exceed maximum limit set
		while (((++pageNoToProcess + 1) * pageSize) <= maxNoOfCandidatesToProcess) {
			//fetch candidates from index
			searchResults = (RemoteSearchResults) searcher.searchCandidates(candidateSearchCritera, pageNoToProcess, pageSize);
			
			//proceed only if some candidates found from index
			if (searchResults != null && searchResults.getCount() > 0) {
				for (int index = 0; index < searchResults.getHitList().size(); index++) {
					Map userValueMap = searchResults.get(index);
					String loginId = ((String) userValueMap.get("LoginId")).trim();
					boolean mailSent = false;
					if (mailSentCheckRequired) {
						mailSent = jdbcUtils.getAlreadySentMailForCombinationIdAndDayDiffValue(
								loginId, JOBAlertConstants.BIRTHDAY_WISH_MAILER_COMBINATION_ID, 1);
					}
					//if mail was not sent previously, sent the mail to him
					if (!mailSent) {
						try {
							//get the candidate info against the loginId and send him a mail
							candidates = jdbcUtils.getResults(typeDetail, CommonSqlQueries.BIRTHDAY_CANDIDATE_DETAILS,
									new Object[] { loginId }, JOBAlertConstants.CANDIDATE_LIVE_DB);
							if (candidates != null && candidates.size() > 0) {
								candidate = candidates.get(0);
								Long trackingId = new Long(tracker.nextLongValue());
								String trackingPixel = MailContentProvider.BASE_URL + 
										"/candidate/track.html?trackingId=" + trackingId.toString();
								Map<String, Object> model = new HashMap<String, Object>();
								model.put("trackingPixel", trackingPixel);
								model.put("firstName", candidate.getAttribute1());
								model.put("horoscopeURL", MailContentProvider.BASE_URL
										+ "/candidate/AutoLogin.html?target=Direct&alt="
										+ EncDecUtil.encrypt(loginId) + "&fromTJ=alert&urlWithParams=" 
										+ encryptedHoroscopeUrl);
								model.put("tjlogoURL", MailContentProvider.BASE_URL
										+ "/candidate/AutoLogin.html?target=Direct&alt="
										+ EncDecUtil.encrypt(loginId) + "&fromTJ=alert&urlWithParams="
										+ encryptedTjLogoUrl);
								String subject = SUBJECT_FOR_BIRTHDAY_WISH_MAILER.replaceAll("<FNAME>",candidate.getAttribute1().toString());
								logger.error("Subject :" + subject);
								String mailContent = 
										contentProvider.getMessageContentForList(BIRTHDAY_WISH_TEMPLATE_NAME, model);
								sender.sendMessage(FROM_ADDRESS, ((String) candidate.getAttribute2()), 
										subject, mailContent);
								jdbcUtils.storeMailTracker(trackingId, loginId,
										JOBAlertConstants.BIRTHDAY_WISH_MAILER_COMBINATION_ID,
										JOBAlertConstants.BIRTHDAY_WISH_MAILER_TEMPLATE_ID,
										subject);
								mailsSentCount++;
								logger.error("BirthDay mail sent to: " + loginId);
							} else {
								logger.error("BirthDay mail could not be sent to: " + loginId  +
										" as corresponding details does not exits at databse");
								mailsFailedtCount++;
							}
						} catch (Exception e) {
							mailsFailedtCount++;
							logger.error("BirthDay mail could not be sent to: " + loginId);
						}
					}
				}
				//update in the database that this page has been processed
				jdbcUtils.storeStatsData("BirthDayWish" + " " + dateFormat.format(currentDate), 
						lastRunID + 1, PAGE_NO_KEY, null, JDBCUtils.RUN_STATE_STATUS, pageNoToProcess);
			} else {
				//if no candidate is found in this fetch from index, exit the processing
				break;
			}
			//after processing the first page there is not required to check for each record 
			//if mail was sent previously to it.
			mailSentCheckRequired = false;
		}
		logger.error("BirthDay mailer finished processing.");
		logger.error("Total no of sent mails : " + mailsSentCount);
		logger.error("Total no of failed mails : " + mailsFailedtCount);
		return RepeatStatus.FINISHED;
	}
	
	public void setJdbcUtils(JDBCUtils jdbcUtils) {
		this.jdbcUtils = jdbcUtils;
	}

	public void setSearcher(CandidateSearcher searcher) {
		this.searcher = searcher;
	}

	public void setTracker(DataFieldMaxValueIncrementer tracker) {
		this.tracker = tracker;
	}

	public void setSender(Sender sender) {
		this.sender = sender;
	}

	public void setContentProvider(MailContentProvider contentProvider) {
		this.contentProvider = contentProvider;
	}

	public void setMinAgeOfCandidate(int minAgeOfCandidate) {
		this.minAgeOfCandidate = minAgeOfCandidate;
	}

	public void setMaxAgeOfCandidate(int maxAgeOfCandidate) {
		this.maxAgeOfCandidate = maxAgeOfCandidate;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public void setMaxNoOfCandidatesToProcess(int maxNoOfCandidatesToProcess) {
		this.maxNoOfCandidatesToProcess = maxNoOfCandidatesToProcess;
	}

	public CandidateSearchCritera getCandidateSearchCritera() {
		return candidateSearchCritera;
	}

	public void setCandidateSearchCritera(
			CandidateSearchCritera candidateSearchCritera) {
		this.candidateSearchCritera = candidateSearchCritera;
	}
}
