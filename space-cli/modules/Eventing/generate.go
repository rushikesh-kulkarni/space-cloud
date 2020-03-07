package eventing

import (
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/spaceuptech/space-cli/model"
)

func generateEventingRule() (*model.SpecObject, error) {
	project := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter Project ID"}, &project); err != nil {
		return nil, err
	}
	ruleType := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter rule type"}, &ruleType); err != nil {
		return nil, err
	}

	v := &model.SpecObject{
		API:  "/v1/config/projects/{project}/eventing/rules/{type}",
		Type: "eventing-rule",
		Meta: map[string]string{
			"type":    ruleType,
			"project": project,
		},
		Spec: map[string]interface{}{
			"rules": map[string]interface{}{
				"create": map[string]interface{}{
					"rule": "allow",
				},
				"delete": map[string]interface{}{
					"rule": "allow",
				},
				"read": map[string]interface{}{
					"rule": "allow",
				},
				"update": map[string]interface{}{
					"rule": "allow",
				},
			},
		},
	}

	return v, nil
}

func generateEventingSchema() (*model.SpecObject, error) {
	project := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter Project ID"}, &project); err != nil {
		return nil, err
	}
	ruleType := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter rule type"}, &ruleType); err != nil {
		return nil, err
	}
	schema := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter Schema"}, &schema); err != nil {
		return nil, err
	}

	v := &model.SpecObject{
		API:  "/v1/config/projects/{project}/eventing/schema/{type}",
		Type: "eventing-schema",
		Meta: map[string]string{
			"type":    ruleType,
			"project": project,
		},
		Spec: map[string]interface{}{
			"schema": schema,
		},
	}

	return v, nil
}

func generateEventingConfig() (*model.SpecObject, error) {
	project := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter project"}, &project); err != nil {
		return nil, err
	}
	dbAlias := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter DB Alias"}, &dbAlias); err != nil {
		return nil, err
	}
	collection := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter colection"}, &collection); err != nil {
		return nil, err
	}

	v := &model.SpecObject{
		API:  "/v1/config/projects/{project}/eventing/config",
		Type: "eventing-config",
		Meta: map[string]string{
			"project": project,
		},
		Spec: map[string]interface{}{
			"dbAlias": dbAlias,
			"col":     collection,
			"enabled": true,
		},
	}

	return v, nil
}

func generateEventingTrigger() (*model.SpecObject, error) {
	project := ""
	if err := survey.AskOne(&survey.Input{Message: "Enter project"}, &project); err != nil {
		return nil, err
	}
	triggerName := ""
	if err := survey.AskOne(&survey.Input{Message: "trigger name"}, &triggerName); err != nil {
		return nil, err
	}

	source := ""
	if err := survey.AskOne(&survey.Select{Message: "Select source ", Options: []string{"Database", "File Storage", "Custom"}}, &source); err != nil {
		return nil, err
	}
	operationType := ""
	var dbType string
	col := ""
	var options interface{}
	switch source {
	case "Database":

		if err := survey.AskOne(&survey.Select{Message: "Select trigger operation", Options: []string{"DB_INSERT", "DB_UPDATE", "DB_DELETE"}}, &operationType); err != nil {
			return nil, err
		}

		if err := survey.AskOne(&survey.Select{Message: "Select database choice ", Options: []string{"mongo", "mysql", "postgres", "sqlserver", "embedded"}}, &dbType); err != nil {
			return nil, err
		}

		if err := survey.AskOne(&survey.Input{Message: "Enter collection/table name"}, &col); err != nil {
			return nil, err
		}
		options = map[string]interface{}{"db": dbType, "col": col}
	case "File Storage":
		if err := survey.AskOne(&survey.Select{Message: "Select trigger operation", Options: []string{"FILE_CREATE", "FILE_DELETE"}}, &operationType); err != nil {
			return nil, err
		}
	case "Custom":
		if err := survey.AskOne(&survey.Input{Message: "Enter trigger type"}, &operationType); err != nil {
			return nil, err
		}
	}
	url := ""
	if err := survey.AskOne(&survey.Input{Message: "webhook url"}, &url); err != nil {
		return nil, err
	}
	wantAdvancedSettings := ""
	if err := survey.AskOne(&survey.Input{Message: "Do you want advanced settings? (Y / n) ?", Default: "n"}, &wantAdvancedSettings); err != nil {
		return nil, err
	}
	retries := "3"
	timeout := "5000"

	if strings.ToLower(wantAdvancedSettings) == "y" {

		if err := survey.AskOne(&survey.Input{Message: "Retries count", Default: "3"}, &retries); err != nil {
			return nil, err
		}

		if err := survey.AskOne(&survey.Input{Message: "Enter Timeout", Default: "5000"}, &timeout); err != nil {
			return nil, err
		}

	}

	v := &model.SpecObject{
		API:  "/v1/config/projects/{project}/eventing/triggers/{triggerName}",
		Type: "eventing-trigger",
		Meta: map[string]string{
			"project":     project,
			"triggerName": triggerName,
		},
		Spec: map[string]interface{}{
			"type":    operationType,
			"url":     url,
			"retries": retries,
			"timeout": timeout,
			"options": options,
		},
	}

	return v, nil
}
