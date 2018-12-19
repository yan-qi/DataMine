package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PlaySessionCount extends NestedListOperation {

    public static final Logger LOG = LoggerFactory.getLogger(PlaySessionCount.class);
    private List<Integer> sessionIds;
    private final long timeoutInMS;
    private final long minDurationInSec;
    private final Column durationCol;
    private final Column startTimestampCol;

    public PlaySessionCount(Column target,
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

        if (size == 0) {
            return;
        }

        //1. init
        long preTs = (long) startTimestampList.get(0);
        Object preUnit = unitIdList.get(0);
        double preDuration = (double) durationList.get(0);
        int curSessionId = 0;
        int startPos = 0;
        boolean isValidSession = preDuration >= minDurationInSec;

        //2. iterate all events to identify all sessions
        for (int i=1; i<size; ++i) {
            //2.1 a new session is found
            if (((long)startTimestampList.get(i) - (preTs + preDuration * 1000)) > timeoutInMS ||
                !Objects.equals(preUnit, unitIdList.get(i))) {
                // a new session
                int id = isValidSession ? curSessionId : -1;
                for (int j=startPos; j<i; j++) {
                    sessionIds.add(id);
                }
                startPos = i;
                preUnit = unitIdList.get(i);
                curSessionId += 1;
                preDuration = (double) durationList.get(i);
                isValidSession = preDuration >= minDurationInSec;
            } else {
                preDuration = (double) durationList.get(i);
                isValidSession |= preDuration >= minDurationInSec;
            }

            //2.2 update the session id
            preTs = (long) startTimestampList.get(i);
        }

        //3. dealing with the potential open session (there might be always ONE open session)
        int id = isValidSession ? curSessionId : -1;
        for (int j=startPos; j<size; j++) {
            sessionIds.add(id);
        }
    }

    @Override
    public Value apply(Row input) throws Exception {

        //1. get from cache if possible
        if (hasCache && cache.containsKey(getID())) {
            return cache.get(getID());
        }

        //2. prepare for initialization
//        try {
//            List targetList1 = (List) targetCol.applyOperation(input).getValue();
//        } catch (ClassCastException ex) {
//            LOG.info(String.format("Row is: %s", input.toString()));
//            LOG.info(String.format("targetCol is: %s", targetCol.getID()));
//            throw ex;
//        }

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

        distincts.remove(-1);

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
            "SessionCount on col(%s) with ts(%s), duration(%s), predicate(%s), min duration (%d), limit (%d) and timeout (%d)",
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
