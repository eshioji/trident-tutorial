package tutorial.storm.trident.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.*;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ContentExtracter {
    private static final Logger log = LoggerFactory.getLogger(ContentExtracter.class);

    public Set<Content> extract(Status tweet) {
        Comparator<Content> byId = new Comparator<Content>() {
            @Override
            public int compare(Content content1, Content content2) {
                return content1.getContentId().compareTo(content2.getContentId());
            }
        };
        Set<Content> contents = new TreeSet<Content>(byId);

        // mentioned accounts
        if (tweet.getUserMentionEntities() != null) {
            extractMentionedAccounts(tweet, contents);
        }

        // retweeted account
        Status retweeted = tweet.getRetweetedStatus();
        if (retweeted != null) {
            extractRetweetedAccounts(tweet, contents, retweeted);
            extractRetweetedStatuses(tweet, contents, retweeted);
        }

        extractRepliedAccount(tweet, contents);
        extractRepliedToStatus(tweet, contents);

        extractUrls(tweet, contents);

        if (tweet.getHashtagEntities() != null) {
            extractHashtags(tweet, contents);
        }

        if (tweet.getSource() != null) {
            Content source = newBase(tweet);
            String sourceNormalized = tweet.getSource();
            source.setContentName(sourceNormalized);
            source.setContentType("source");
            contents.add(source);
        }

        if (tweet.getPlace() != null) {
            Content place = newBase(tweet);
            place.setContentName(tweet.getPlace().getCountryCode());
            place.setContentType("place_country-code");
            contents.add(place);
        }

        if (tweet.getPlace() != null) {
            Content place = newBase(tweet);
            String placeNormalized = tweet.getPlace().getFullName();
            place.setContentName(placeNormalized);
            place.setContentType("place_fullname");
            contents.add(place);
        }

        return contents;
    }

    private void extractUrls(Status tweet, Set<Content> contents) {
        for (URLEntity urlEntity : tweet.getURLEntities()) {
            String url = urlEntity.getExpandedURL();
            url = url == null ? urlEntity.getURL() : url;
            Content shareUrl = newBase(tweet);
            shareUrl.setContentName(url);
            shareUrl.setContentType("url");
            contents.add(shareUrl);
        }
    }


    private void extractRepliedToStatus(Status tweet, Set<Content> contents) {
        long statusId = tweet.getInReplyToStatusId();
        if (statusId > 0) {
            Content replyStatus = newBase(tweet);
            replyStatus.setContentName(String.valueOf(statusId));
            replyStatus.setContentType("reply_to_status");
            contents.add(replyStatus);
        }
    }

    private void extractRepliedAccount(Status tweet, Set<Content> contents) {
        long accountId = tweet.getInReplyToUserId();
        if (accountId > 0) {
            Content replyStatus = newBase(tweet);
            replyStatus.setContentName(String.valueOf(accountId));
            replyStatus.setContentType("reply_to_account");
            contents.add(replyStatus);
        }
    }

    private void extractHashtags(Status tweet, Set<Content> contents) {
        for (HashtagEntity entity : tweet.getHashtagEntities()) {
            Content hashtag = newBase(tweet);
            String hashtag_normalized = entity.getText().toLowerCase();
            hashtag.setContentName(hashtag_normalized);
            hashtag.setContentType("hashtag");
            contents.add(hashtag);
        }
    }

    private void extractRetweetedAccounts(Status tweet, Set<Content> contents, Status retweeted) {
        if (retweeted != null) {
            Content retweetedUser = newBase(tweet);
            retweetedUser.setContentName(String.valueOf(retweeted.getUser().getId()));
            retweetedUser.setContentType("account_retweeted");
            contents.add(retweetedUser);
        }
    }

    private void extractRetweetedStatuses(Status tweet, Set<Content> contents, Status retweeted) {
        if (retweeted != null) {
            Content retweetedStatus = newBase(tweet);
            retweetedStatus.setContentName(String.valueOf(retweeted.getId()));
            retweetedStatus.setContentType("status_retweeted");
            contents.add(retweetedStatus);
        }
    }

    private void extractMentionedAccounts(Status tweet, Set<Content> contents) {
        for (UserMentionEntity entity : tweet.getUserMentionEntities()) {
            Content mentionedUser = newBase(tweet);
            mentionedUser.setContentName(String.valueOf(entity.getId()));
            mentionedUser.setContentType("account_mentioned");
            contents.add(mentionedUser);
        }
    }


    private Content newBase(Status tweet) {
        return new Content(
                tweet.getId(),
                tweet.getUser().getId(),
                tweet.getCreatedAt().getTime()
        );
    }

}
