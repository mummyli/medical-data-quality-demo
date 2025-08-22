from abc import ABC, abstractmethod
from typing import Dict, List, Any


class RuleStrategy(ABC):
    @abstractmethod
    def check(self, event: Dict, rule: Dict) -> str:
        """check the rule, return error message, and return None if passed"""
        pass


class FieldExistsRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        if not event.get(field):
            return f"{rule['id']}: {rule['description']}"
        return None
    
    
class RequiredRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        if not event.get(field) and len(event.get(field)) > 0:
            return f"{rule['id']}: {rule['description']}"
        return None


class ValueLengthRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        params = rule.get('params', {})
        value = event.get(field)
        try:
            value_length = len(value)
            min_val = params.get('min', -10**9)
            max_val = params.get('max', 10**9)
            
            if value_length < min_val or value_length > max_val:
                return f"{rule['id']}: {rule['description']}"
        except (ValueError, TypeError):
            return f"{rule['id']}: {rule['description']}"
        
        return None
        

class NumberRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        value = event.get(field)
        
        try:
            value = int(value)
        except (ValueError, TypeError):
            return f"{rule['id']}: {rule['description']}"
        
        return None
    
           
class RangeRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        params = rule.get('params', {})
        value = event.get(field)
        
        try:
            value = int(value)
            min_val = params.get('min', -10**9)
            max_val = params.get('max', 10**9)
            
            if value < min_val or value > max_val:
                return f"{rule['id']}: {rule['description']}"
        except (ValueError, TypeError):
            return f"{rule['id']}: {rule['description']}"
        
        return None
    


class CrossFieldRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        params = rule.get('params', {})
        left_field = params.get('left')
        right_field = params.get('right')
        
        if left_field and right_field and event.get(left_field) and event.get(right_field):
            if event[left_field] > event[right_field]:
                return f"{rule['id']}: {rule['description']}"
        
        return None

class RuleStrategyFactory:
    _strategies: Dict[str, RuleStrategy] = {
        'field_exist': FieldExistsRuleStrategy(),
        'required': RequiredRuleStrategy(),
        'value_length': ValueLengthRuleStrategy(),
        'number_filed': NumberRuleStrategy(),
        'range': RangeRuleStrategy(),
        'cross_field': CrossFieldRuleStrategy()
    }
    
    @classmethod
    def get_strategy(cls, rule_type: str) -> RuleStrategy:
        strategy = cls._strategies.get(rule_type)
        if not strategy:
            raise ValueError(f"Not support rule type: {rule_type}")
        return strategy
    
    @classmethod
    def register_strategy(cls, rule_type: str, strategy: RuleStrategy):
        cls._strategies[rule_type] = strategy

def apply_rules(event: Dict, rules: List[Dict]) -> List[str]:
    failures = []
    
    for rule in rules:
        rule_type = rule.get('type')
        try:
            strategy = RuleStrategyFactory.get_strategy(rule_type)
            error = strategy.check(event, rule)
            if error:
                failures.append(error)
        except ValueError as e:
            failures.append(str(e))
    
    return failures

