package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/spaceuptech/space-cloud/gateway/config"
	"github.com/spaceuptech/space-cloud/gateway/model"
	"github.com/spaceuptech/space-cloud/gateway/utils"
)

func (m *Module) matchRule(ctx context.Context, project string, rule *config.Rule, args, auth map[string]interface{}) (*model.PostProcess, error) {
	if project != m.project {
		return &model.PostProcess{}, errors.New("invalid project details provided")
	}

	if rule.Rule == "allow" || rule.Rule == "authenticated" {
		return &model.PostProcess{}, nil
	}

	if idTemp, p := auth["id"]; p {
		if id, ok := idTemp.(string); ok && id == utils.InternalUserID {
			return &model.PostProcess{}, nil
		}
	}

	switch rule.Rule {
	case "deny":
		return &model.PostProcess{}, ErrIncorrectMatch

	case "match":
		return &model.PostProcess{}, match(rule, args)

	case "and":
		return m.matchAnd(ctx, project, rule, args, auth)

	case "or":
		return m.matchOr(ctx, project, rule, args, auth)

	case "webhook":
		return &model.PostProcess{}, m.matchFunc(ctx, rule, m.makeHttpRequest, args)

	case "query":
		return &model.PostProcess{}, matchQuery(ctx, project, rule, m.crud, args)

	case "force":
		return matchForce(rule, args)

	case "remove":
		return matchRemove(rule, args)
	default:
		return &model.PostProcess{}, ErrIncorrectMatch
	}
}

func (m *Module) matchFunc(ctx context.Context, rule *config.Rule, MakeHttpRequest utils.MakeHttpRequest, args map[string]interface{}) error {
	obj := args["args"].(map[string]interface{})
	token := obj["token"].(string)
	delete(obj, "token")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	scToken, err := m.GetSCAccessToken()
	if err != nil {
		return err
	}

	var result interface{}
	return MakeHttpRequest(ctx, "POST", rule.URL, token, scToken, obj, &result)
}

func matchQuery(ctx context.Context, project string, rule *config.Rule, crud model.CrudAuthInterface, args map[string]interface{}) error {
	// Adjust the find object to load any variables referenced from state
	rule.Find = utils.Adjust(rule.Find, args).(map[string]interface{})

	// Create a new read request
	req := &model.ReadRequest{Find: rule.Find, Operation: utils.One}

	// Execute the read request
	_, err := crud.Read(ctx, rule.DB, project, rule.Col, req)
	return err
}

func (m *Module) matchAnd(ctx context.Context, projectID string, rule *config.Rule, args, auth map[string]interface{}) (*model.PostProcess, error) {
	completeAction := &model.PostProcess{}
	for _, r := range rule.Clauses {
		postProcess, err := m.matchRule(ctx, projectID, r, args, auth)
		// if err is not nil then return error without checking the other clauses.
		if err != nil {
			return &model.PostProcess{}, err
		}
		completeAction.PostProcessAction = append(completeAction.PostProcessAction, postProcess.PostProcessAction...)
	}
	return completeAction, nil
}

func (m *Module) matchOr(ctx context.Context, projectID string, rule *config.Rule, args, auth map[string]interface{}) (*model.PostProcess, error) {
	//append all parameters returned by all clauses! and then return mainStruct
	for _, r := range rule.Clauses {
		postProcess, err := m.matchRule(ctx, projectID, r, args, auth)
		if err == nil {
			//if condition is satisfied -> exit the function
			return postProcess, nil
		}
	}
	//if condition is not satisfied -> return empty model.PostProcess and error
	return &model.PostProcess{}, ErrIncorrectMatch
}

func match(rule *config.Rule, args map[string]interface{}) error {
	switch rule.Type {
	case "string":
		return matchString(rule, args)

	case "number":
		return matchNumber(rule, args)

	case "bool":
		return matchBool(rule, args)
	}

	return ErrIncorrectMatch
}

func matchForce(rule *config.Rule, args map[string]interface{}) (*model.PostProcess, error) {
	value := rule.Value
	if stringValue, ok := rule.Value.(string); ok {
		loadedValue, err := utils.LoadValue(stringValue, args)
		if err == nil {
			value = loadedValue
		}
	}
	//"res" - add to structure for post processing || "args" - store in args
	if strings.HasPrefix(rule.Field, "res") {
		addToStruct := model.PostProcessAction{Action: "force", Field: rule.Field, Value: value}
		return &model.PostProcess{PostProcessAction: []model.PostProcessAction{addToStruct}}, nil
	} else if strings.HasPrefix(rule.Field, "args") {
		err := utils.StoreValue(rule.Field, value, args)
		return &model.PostProcess{}, err
	} else {
		return nil, ErrIncorrectRuleFieldType
	}
}

func matchRemove(rule *config.Rule, args map[string]interface{}) (*model.PostProcess, error) {
	actions := &model.PostProcess{}
	for _, field := range rule.Fields {
		//"res" - add field to structure for post processing || "args" - delete field from args
		if strings.HasPrefix(field, "res") {
			addToStruct := model.PostProcessAction{Action: "remove", Field: field, Value: nil}
			actions.PostProcessAction = append(actions.PostProcessAction, addToStruct)
		} else if strings.HasPrefix(field, "args") {
			// Since it depends on the request itself, delete the field from args
			if err := utils.DeleteValue(field, args); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrIncorrectRuleFieldType
		}
	}
	return actions, nil
}
