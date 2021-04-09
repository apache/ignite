from ducktape.tests.test import Test
from ducktest_ex.services.example_class import ExampleClass

class ClassVisibilityTest(Test):
    def test_class_visibility(self):
        var = ExampleClass()
