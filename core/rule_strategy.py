from abc import ABC, abstractmethod
from typing import Dict, List, Any
import re
import datetime
from core.lookup_loader import get_lookup_record, get_lookup_table



class RuleStrategy(ABC):
    @abstractmethod
    def check(self, event: Dict, rule: Dict) -> str:
        """check the rule, return error message, and return None if passed"""
        pass


class FieldExistsRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        # Only check if field key exists, value can be empty
        if not event.get(field):
            return f"{rule['id']}: {rule['description']}"
        return None
    
    
class RequiredRuleStrategy(RuleStrategy):
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        # Must exist and not empty
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
        compare = params.get('compare', '==')
        
        left_val = event.get(left_field)
        right_val = event.get(right_field)

        if left_val is None or right_val is None:
            return None

        try:
            if compare == "<=" and not (left_val <= right_val):
                return f"{rule['id']}: {rule['description']}"
            elif compare == "<" and not (left_val < right_val):
                return f"{rule['id']}: {rule['description']}"
            elif compare == ">=" and not (left_val >= right_val):
                return f"{rule['id']}: {rule['description']}"
            elif compare == ">" and not (left_val > right_val):
                return f"{rule['id']}: {rule['description']}"
            elif compare == "==" and not (left_val == right_val):
                return f"{rule['id']}: {rule['description']}"
        except Exception:
            return f"{rule['id']}: {rule['description']}"
        return None

class RegexDateRuleStrategy(RuleStrategy):
    """Check date field with regex and valid date parsing"""
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        value = event.get(field)
        pattern = rule.get('pattern_regex')
        if not value:
            return f"{rule['id']}: {rule['description']}"

        # regex check
        if pattern and not re.fullmatch(pattern, str(value)):
            return f"{rule['id']}: {rule['description']}"

        # try parse as valid date
        try:
            datetime.datetime.strptime(value, "%Y-%m-%d")
        except Exception:
            return f"{rule['id']}: {rule['description']}"
        return None


class RegexTimeRuleStrategy(RuleStrategy):
    """Check time field with regex and valid time parsing"""
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        value = event.get(field)
        pattern = rule.get('pattern_regex')
        if not value:
            return f"{rule['id']}: {rule['description']}"

        # regex check
        if pattern and not re.fullmatch(pattern, str(value)):
            return f"{rule['id']}: {rule['description']}"

        # try parse as valid time
        try:
            datetime.datetime.strptime(value, "%H:%M:%S")
        except Exception:
            return f"{rule['id']}: {rule['description']}"
        return None


class RegexDateTimeRuleStrategy(RuleStrategy):
    """Check datetime field with regex and valid datetime parsing"""
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        value = event.get(field)
        pattern = rule.get('pattern_regex')
        if not value:
            return f"{rule['id']}: {rule['description']}"

        # regex check
        if pattern and not re.fullmatch(pattern, str(value)):
            return f"{rule['id']}: {rule['description']}"

        # try parse as valid ISO datetime
        try:
            datetime.datetime.fromisoformat(value)
        except Exception:
            return f"{rule['id']}: {rule['description']}"
        return None


class DomainCheckRuleStrategy(RuleStrategy):
    """Check if value is within given domain list"""
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('filed') or rule.get('field')
        value = event.get(field)
        allowed = rule.get('code', "")
        allowed_set = set([x.strip() for x in allowed.split(",")])
        if value not in allowed_set:
            return f"{rule['id']}: {rule['description']}"
        return None


class LookupExistsRuleStrategy(RuleStrategy):
    """Check if value exists in a lookup table (loaded by lookup_loader)"""
    def check(self, event: Dict, rule: Dict) -> str:
        field = rule.get('field')
        lookup = rule.get('lookup', {})
        lookup_name = lookup.get('name')
        key_field = lookup.get('key')  # optional override

        if not lookup_name:
            return f"{rule['id']}: {rule['description']}"

        # value to lookup: use event[field] or if key mapping provided, event[key_field]
        value = event.get(field) if not key_field else event.get(key_field)
        if value is None:
            return f"{rule['id']}: {rule['description']}"

        ref = get_lookup_record(lookup_name, value)
        print(f"loading {lookup_name} success: {len(ref)}")
        if not ref:
            return f"{rule['id']}: {rule['description']}"
        return None


class TimeDiffRuleStrategy(RuleStrategy):
    """Check if two datetime fields are within max_seconds using lookup table"""
    def check(self, event: Dict, rule: Dict) -> str:
        lookup = rule.get('lookup', {})
        lookup_name = lookup.get('name')
        params = rule.get('params', {})
        if not lookup_name or not params:
            return f"{rule['id']}: {rule['description']}"

        # key_map example: { visit_id: visit_id }
        key_map = params.get('key_map', {})
        # for simplicity assume single mapping like visit_id -> visit_id
        if not key_map:
            return f"{rule['id']}: {rule['description']}"

        # pick first mapping
        mapping_event_key = list(key_map.values())[0]
        mapping_lookup_key = list(key_map.keys())[0]
        visit_id = event.get(mapping_event_key)
        if visit_id is None:
            return None  # can't evaluate

        ref = get_lookup_record(lookup_name, visit_id)
        print(f"loading {lookup_name} success: {len(ref)}")
        if not ref:
            return None  # no reference record -> treat as pass (or change policy)

        left_time_key = params.get('left_time')
        right_time_key = params.get('right_time')
        max_seconds = params.get('max_seconds', 0)

        left_time = event.get(left_time_key)
        right_time = ref.get(right_time_key)
        if not left_time or not right_time:
            return f"{rule['id']}: {rule['description']}"

        try:
            t1 = datetime.datetime.fromisoformat(left_time)
            t2 = datetime.datetime.fromisoformat(right_time)
            if abs((t1 - t2).total_seconds()) > max_seconds:
                return f"{rule['id']}: {rule['description']}"
        except Exception:
            return f"{rule['id']}: {rule['description']}"
        return None


class ReconciliationRuleStrategy(RuleStrategy):
    """
    Compare event fields with a referenced record.
    The rule.params is a list of comparisons. We use lookup_name + a mapping to find the referenced record.
    """
    def check(self, event: Dict, rule: Dict) -> str:
        lookup = rule.get('lookup', {})
        lookup_name = lookup.get('name')
        params = rule.get('params', [])

        if not lookup_name or not params:
            return f"{rule['id']}: {rule['description']}"

        # try to find a key in params to fetch the referenced record
        # prefer explicit key mapping, else try event.patient_id
        # (adapt to your rule shape: CT102 in your sample does not provide explicit key_map, so user may need to extend.
        #  For that case we can try to use patient_id from event)
        ref_key = event.get('patient_id') or event.get('visit_id')
        ref = get_lookup_record(lookup_name, ref_key) if ref_key else None
        print(f"loading {lookup_name} success: {len(ref)}")

        # If no per-key match, try to get the whole table and scan (only when small / loaded)
        if not ref:
            table = get_lookup_table(lookup_name)
            if table:
                # scan table for the first record that matches join conditions (simple heuristic)
                for candidate in table.values():
                    ok = True
                    for cond in params:
                        if cond.get('type') == 'equality':
                            left = event.get(cond.get('left'))
                            right = candidate.get(cond.get('right'))
                            if left != right:
                                ok = False
                                break
                    if ok:
                        ref = candidate
                        break

        if not ref:
            # no reference found -> depending on policy, either pass or fail; here we return fail
            return f"{rule['id']}: {rule['description']}"

        # now evaluate the comparisons
        for cond in params:
            typ = cond.get('type')
            if typ == 'equality':
                left = event.get(cond.get('left'))
                right = ref.get(cond.get('right'))
                if left != right:
                    return f"{rule['id']}: {rule['description']}"
            elif typ == 'compare':
                left = event.get(cond.get('left'))
                right = ref.get(cond.get('right'))
                cmp = cond.get('comparetor') or cond.get('comparator') or '<='
                try:
                    if cmp == '<=' and not (left <= right):
                        return f"{rule['id']}: {rule['description']}"
                    # add others as needed
                except Exception:
                    return f"{rule['id']}: {rule['description']}"
        return None


class RuleStrategyFactory:
    _strategies: Dict[str, RuleStrategy] = {
        'field_exist': FieldExistsRuleStrategy(),
        'required': RequiredRuleStrategy(),
        'value_length': ValueLengthRuleStrategy(),
        'number_filed': NumberRuleStrategy(),
        'range': RangeRuleStrategy(),
        'cross_field': CrossFieldRuleStrategy(),
        'valid_date': RegexDateRuleStrategy(),
        'valid_time': RegexTimeRuleStrategy(),
        'valid_datetime': RegexDateTimeRuleStrategy(),
        'domain_check': DomainCheckRuleStrategy(),
        'lookup_exists': LookupExistsRuleStrategy(),
        'reconciliation': ReconciliationRuleStrategy(),
        'time_diff': TimeDiffRuleStrategy()
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

