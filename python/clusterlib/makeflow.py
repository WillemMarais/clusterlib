"""Python wrapper of `The Cooperative Computing Lab` makflow tool:
https://ccl.cse.nd.edu/software/manuals/makeflow.html. The classes produce JX formatted text;
the specifications of the JX format is defined
in: https://ccl.cse.nd.edu/software/manuals/jx.html."""

from typing import List, Union


class Category:
    """Specify computing resources; specific information about the computing resources are
    described in https://ccl.cse.nd.edu/software/manuals/makeflow.html#rescat."""

    def __init__(self, category_name_str: str, cores_int: int, mem_MB_int: int):
        """

        Parameters
        ----------
        category_name_str: str
            The category name.
        cores_int: int
            The number of CPU cores for the process.
        mem_MB_int: int
            The amount of mega-bytes of memory that is available for the process.
        """

        self.category_name_str = category_name_str
        self.cores_int = cores_int
        self.mem_MB_int = mem_MB_int

    def __str__(self):
        category_str = """"{:s}": {{
    "resources": {{"cores": {:d}, "memory": {:d}}}
}}"""

        return category_str.format(self.category_name_str, self.cores_int, self.mem_MB_int)


class Environment:
    """Specify the environment variables; specific information about the environment variables are
    described in https://ccl.cse.nd.edu/software/manuals/jx.html."""

    def __init__(self, env_name_str: str, value_str: str):
        """

        Parameters
        ----------
        env_name_str: str
            The environmental variable name.
        value_str: str
            The value of the environmental variable.
        """

        self.env_name_str = env_name_str
        self.value_str = value_str

    def __str__(self):
        environment_str = '{:s}: "{:s}"'

        return environment_str.format(self.env_name_str, self.value_str)


class Rule:
    """Computing rule; specific information about the computing rule is described in
    https://ccl.cse.nd.edu/software/manuals/makeflow.html#rescat."""

    def __init__(self, range_letter_str: str = None, range_start_int: int = None, range_end_int: int = None,
                 category_obj: Category = None):
        """
        Parameters
        ----------
        range_letter_str: str
            The range letter that is used in command string. If `range_letter_str` is None,
            then no range is set in the Makeflow JX file.
        range_start_int: int
            The starting number of the range.
        range_end_int: int
            The last number of the range.
        category_obj: Category
            The resources category associated with this rule.
        """

        self.range_letter_str = range_letter_str

        if range_start_int is None and isinstance(self.range_letter_str, str):
            err_str = 'If the "range_letter_str" is set, then "range_start_int" to be set also.'
            raise ValueError(err_str)
        else:
            self.range_start_int = range_start_int

        if range_end_int is None and isinstance(self.range_letter_str, str):
            err_str = 'If the "range_letter_str" is set, then "range_end_int" to be set also.'
            raise ValueError(err_str)
        else:
            self.range_end_int = range_end_int

        self.category_obj = category_obj

        self.cmd_str = None
        self.input_fileP_str_lst = None
        self.output_fileP_str_lst = None

    def set_command(self, cmd_str: str, input_fileP_str_lst: List[str] = None, output_fileP_str_lst: List[str] = None):
        """Set the command that will be executed by this rule.

        Parameters
        ----------
        cmd_str: str
            The command that will be executed; the appropriate range letter should be used in the
            command string. E.g., <command> N, where N is the range letter.
        input_fileP_str_lst: list of str
            The input files of the rule.
        output_fileP_str_lst: list of str
            The output files of the rule."""

        self.cmd_str = cmd_str
        self.input_fileP_str_lst = input_fileP_str_lst
        self.output_fileP_str_lst = output_fileP_str_lst

    def _replace_range_letter(self, parm_str: str):
        """The makeflow JX has strange formatting requirement to delineate range letter, probably because
        the command string might have a character that is shared with the range letter.

        Parameters
        ----------
        parm_str: str
            The command string that includes the range letter.

        Returns
        -------
        fmrt_cmd_str: str
            The formatted command string with the range letter in it."""

        if isinstance(self.range_letter_str, str) is True:
            fmrt_cmd_str = '"' + parm_str.replace('+' + self.range_letter_str + '+',
                                                  '" + {:s} + "'.format(self.range_letter_str)) + '"'
        else:
            fmrt_cmd_str = '"{:s}"'.format(parm_str)

        return fmrt_cmd_str

    def __str__(self):
        if self.cmd_str is None:
            err_str = 'The function "set_command" has to be called before creating the rule string'
            raise ValueError(err_str)

        # Create the command string
        command_str = '{:s}'.format(self._replace_range_letter(self.cmd_str))

        out_format_str = """{{
    "command": {command_str}"""
        out_format_kwargs_dct = {
            'command_str': command_str
        }

        # Create the inputs string
        if isinstance(self.input_fileP_str_lst, list) is True:
            inputs_str = '['
            inputs_str += ' ' + self._replace_range_letter(self.input_fileP_str_lst[0])
            for input_fileP_str in self.input_fileP_str_lst[1:]:
                inputs_str += ', ' + self._replace_range_letter(input_fileP_str)
            inputs_str += ' ]'

            out_format_str += """,
    "inputs": {inputs_str}"""
            out_format_kwargs_dct['inputs_str'] = inputs_str

        # Create the output string
        if isinstance(self.output_fileP_str_lst, list) is True:
            outputs_str = '['
            outputs_str += ' ' + self._replace_range_letter(self.output_fileP_str_lst[0])
            for output_fileP_str in self.output_fileP_str_lst[1:]:
                outputs_str += ', ' + self._replace_range_letter(output_fileP_str)
            outputs_str += ' ]'

            out_format_str += """,
    "outputs": {outputs_str}"""
            out_format_kwargs_dct['outputs_str'] = outputs_str

        if isinstance(self.category_obj, Category) is True:
            category_str = '"{:s}"'.format(self.category_obj.category_name_str)

            out_format_str += """,
    "category": {category_str}"""
            out_format_kwargs_dct['category_str'] = category_str

        out_format_str += """
}}"""

        if isinstance(self.range_letter_str, str) is True:
            out_format_str += ' for {:s} in range({:d}, {:d})'.format(self.range_letter_str, self.range_start_int,
                                                                      self.range_end_int)

        return out_format_str.format(**out_format_kwargs_dct)


class JxMakeflow:
    """Create JX formatted makeflow strings."""

    def __init__(self):
        self.environment_obj_lst = []
        self.category_obj_lst = []
        self.rule_obj_lst = []

    def add_environment(self, environment_obj: Environment):
        """Add an environment."""

        self.environment_obj_lst.append(environment_obj)

    def add_category(self, category_obj: Union[Category, None]):
        """Add a resource category."""

        if category_obj is not None:
            self.category_obj_lst.append(category_obj)

    def add_rule(self, rule_obj: Rule):
        """Add a Makeflow rule."""

        if rule_obj.category_obj is not None:
            self.category_obj_lst.append(rule_obj.category_obj)

        self.rule_obj_lst.append(rule_obj)

    @staticmethod
    def _indent_lines(str_str, nr_spacesers_int = 0, spacer_str = ' '):
        indent_str = spacer_str * nr_spacesers_int

        return '\n'.join([indent_str + sub_str_str for sub_str_str in str_str.split('\n')])

    def get_str(self, tab_size_int = 4):
        """Create the Makeflow JX formatted string."""

        if len(self.rule_obj_lst) == 0:
            return ''

        out_str = '{'
        if len(self.category_obj_lst) > 0:
            out_str += self._indent_lines('\n"categories": {', tab_size_int)
            out_str += self._indent_lines('\n' + str(self.category_obj_lst[0]), 2 * tab_size_int)

            for category_obj in self.category_obj_lst[1:]:
                out_str += ','
                out_str += self._indent_lines('\n' + str(category_obj), 2 * tab_size_int)

            out_str += self._indent_lines('\n},', tab_size_int)

        if len(self.environment_obj_lst) > 0:
            out_str += self._indent_lines('\n"environment": {', tab_size_int)
            out_str += self._indent_lines('\n' + str(self.environment_obj_lst[0]), 2 * tab_size_int)

            for environment_obj in self.environment_obj_lst:
                out_str += ','
                out_str += self._indent_lines('\n' + str(environment_obj), 2 * tab_size_int)

            out_str += self._indent_lines('\n},', tab_size_int)

        out_str += self._indent_lines('\n"rules": [', tab_size_int)
        out_str += self._indent_lines('\n' + str(self.rule_obj_lst[0]), 2 * tab_size_int)
        for rule_obj in self.rule_obj_lst[1:]:
            out_str += ','
            out_str += self._indent_lines('\n' + str(rule_obj), 2 * tab_size_int)

        out_str += self._indent_lines('\n]', tab_size_int)
        out_str += '\n}'

        return out_str

    def __str__(self):
        return self.get_str(tab_size_int=4)
