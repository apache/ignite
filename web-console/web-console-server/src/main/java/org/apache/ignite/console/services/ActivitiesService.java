

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.event.ActivityEventType.ACTIVITY_UPDATE;

/**
 * Service to handle activities.
 */
@Service
public class ActivitiesService {
    /** */
    private final ActivitiesRepository activitiesRepo;

    /** */
    private final EventPublisher evtPublisher;

    /**
     * @param activitiesRepo Repository to work with activities.
     * @param evtPublisher Event publisher.
     */
    public ActivitiesService(
            ActivitiesRepository activitiesRepo,
            EventPublisher evtPublisher
    ) {
        this.activitiesRepo = activitiesRepo;
        this.evtPublisher = evtPublisher;
    }

    /**
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     */
    public void save(UUID accId, String grp, String act) {
        Activity activity = activitiesRepo.save(accId, grp, act);

        evtPublisher.publish(new Event<>(ACTIVITY_UPDATE, activity));
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     * @return Collection of user activities.
     */
    public Collection<Activity> activitiesForPeriod(UUID accId, long startDate, long endDate) {
        return activitiesRepo.activitiesForPeriod(accId, startDate, endDate);
    }
}
