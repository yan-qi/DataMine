package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PlaySessionCount2 extends NestedListOperation {

    public static final Logger LOG = LoggerFactory.getLogger(PlaySessionCount2.class);
    private List<Integer> sessionIds;
    private final long timeoutInMS;
    private final long minDurationInSec;
    private final Column durationCol;
    private final Column startTimestampCol;

    public PlaySessionCount2(Column target,
                             Column startTimestampCol,
                             Column durationCol,
                             Column predicate,
                             boolean canCache,
                             long minDurationInMS,
                             long limit,
                             long timeoutInMS) {
        super(target, predicate, canCache, limit);
        this.timeoutInMS = timeoutInMS;
        this.durationCol = durationCol;
        this.startTimestampCol = startTimestampCol;
        this.minDurationInSec = minDurationInMS / 1000;
    }

    private void initSessions(List startTimestampList, List durationList, List unitIdList) {

        int size = startTimestampList.size();
        sessionIds = new ArrayList<>(size);

        //1. init
        long preTs = (long) startTimestampList.get(0);
        Object preUnit = unitIdList.get(0);
        double preDuration = (double) durationList.get(0);
        int curSessionId = 0;
        sessionIds.add(curSessionId);

        //2. iterate all events to identify all sessions
        for (int i=1; i<size; ++i) {
            //2.1 a new session is found
            if (((long)startTimestampList.get(i) - (preTs + preDuration * 1000)) > timeoutInMS ||
                !Objects.equals(preUnit, unitIdList.get(i))) {
                // a new session
                curSessionId ++;
                preUnit = unitIdList.get(i);
            }

            //2.2 update the session id
            sessionIds.add(curSessionId);
            preTs = (long) startTimestampList.get(i);
            preDuration = (double) durationList.get(i);
        }
    }

    @Override
    public Value apply(Row input) throws Exception {

        //1. get from cache if possible
        if (hasCache && cache.containsKey(getID())) {
            return cache.get(getID());
        }

        //2. prepare for initialization
        List targetList = (List) targetCol.applyOperation(input).getValue();
        if (sessionIds == null) {
            List durationList = (List) durationCol.applyOperation(input).getValue();
            List tsList = (List) startTimestampCol.applyOperation(input).getValue();
            initSessions(tsList, durationList, targetList);
        }

        Set<Integer> distincts = new HashSet<>(targetList.size());
        if (predicateCol == null) {
            distincts.addAll(sessionIds);
        } else {
            for (int i=0; i < Math.min(targetList.size(), limit); ++i) {
                Value ret = predicateCol.applyOperation(input, i);
                if (ValueUtils.isTrue(ret)) {
                    distincts.add(sessionIds.get(i));
                }
            }
        }

        Value result = new Value(distincts.size(), ValueUtils.LongDataType);
        if (hasCache) {
            cache.put(getID(), result);
        }
        return result;
    }

    @Override
    public int getResultValueType() {
        return ValueUtils.LongDataType;
    }

    @Override
    public String getID() {
        return String.format(
            "Fake SessionCount on col(%s) with ts(%s), duration(%s), predicate(%s), min duration (%d), limit (%d) and timeout (%d)",
            targetCol.getID(),
            startTimestampCol.getID(),
            durationCol.getID(),
            predicateCol == null ? "true" : predicateCol.getID(),
            minDurationInSec, limit, timeoutInMS);
    }

    @Override
    public void cleanCache() {
        super.cleanCache();
        sessionIds = null;
    }
}
