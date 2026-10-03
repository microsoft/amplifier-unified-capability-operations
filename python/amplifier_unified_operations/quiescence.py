"""Operations-specific passive routes over the shared optional Foundation mechanism."""
from amplifier_operations.quiescence import DurableIntakeFence

READ_ACTIONS={
 'schedule.list','schedule.read','schedule.request',
 'operations.list','operations.status','operations.read','operations.wait','operations.request',
 'operations.history.list','operations.history.read','question.list','question.read',
 'observation.observers','observation.request','observation.list','observation.read','observation.report',
}
READ_METHODS={'initialize','actions','snapshot','questionAttention','questionDependency','readTransferEvidence'}

class Intake(DurableIntakeFence):
    def readonly(self,method,args):return method in READ_METHODS or method=='action' and args.get('operation') in READ_ACTIONS
