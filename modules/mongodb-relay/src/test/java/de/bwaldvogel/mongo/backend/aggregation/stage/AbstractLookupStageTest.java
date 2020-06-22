package de.bwaldvogel.mongo.backend.aggregation.stage;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.bwaldvogel.mongo.MongoDatabase;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractLookupStageTest {

    @Mock
    MongoDatabase database;

}
