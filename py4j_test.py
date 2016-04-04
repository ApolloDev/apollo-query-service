from py4j.java_gateway import JavaGateway
from py4j.java_gateway import java_import
gateway = JavaGateway()
java_import(gateway.jvm,'java.math.*')
bigInt = gateway.jvm.BigInteger("5")
simulatorCountOutput = gateway.entry_point.getSimulatorCountOutput()
simulatorCountOutput.setSimulatorCountOutputSpecificationId(bigInt)

obj = gateway.entry_point.serialize()
print(obj)
