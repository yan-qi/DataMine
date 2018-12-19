package datamine.query.parser;

import org.junit.Test;

public class ExampleProfileQueryParserTest {

    ProfileQuery profileQuery = new ProfileQuery(
        "datamine.query.example.model");

    @Test
    public void parsingTable1() {
        String query =
            "sel getDate(creation_time) col1, events.event_time, count(*), count(distinct profile_id) " +
            "from profile, profile.events " +
            "dates [2012/01/02, 2018/02/03] time_zone EST " +
            "where not events.duration > 3 or events.duration < 2 and events.post_time <> 123 or events.url not like 'test'";
        profileQuery.parse(query);
    }

    // parse case when statment
//    sel case
//    when nf_count(profile.events.event_time, profile.events.event_type = 'media' and profile.events.event_time >= ts('2018/09/01')) > 0
//    then concat('2018/09-', monthof(pf_min(profile.events.event_time, profile.events.event_type = 'media')))
//    when nf_count(profile.events.event_time, profile.events.event_type = 'media' and profile.events.event_time >= ts('2018/08/01')) > 0
//    then concat('2018/08-', monthof(pf_min(profile.events.event_time, profile.events.event_type = 'media')))
//    when nf_count(profile.events.event_time, profile.events.event_type = 'media' and profile.events.event_time >= ts('2018/07/01')) > 0
//    then concat('2018/07-', monthof(pf_min(profile.events.event_time, profile.events.event_type = 'media')))
//    when nf_count(profile.events.event_time, profile.events.event_type = 'media' and profile.events.event_time >= ts('2018/06/01')) > 0
//    then concat('2018/06-', monthof(pf_min(profile.events.event_time, profile.events.event_type = 'media')))
//            else 'others' end as first_play_date,
//    count(*)
//    from profile
//    when past 1000 days
}


