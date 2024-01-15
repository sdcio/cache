package cache

import (
	"testing"
)

func Test_pathToPrefixPattern(t *testing.T) {
	type args struct {
		path []string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{
			name: "path_no_wildcard",
			args: args{
				path: []string{"interface", "mgmt0", "admin-state"},
			},
			want:  "interface,mgmt0,admin-state",
			want1: "",
		},
		{
			name: "path_with_wildcard_1",
			args: args{
				path: []string{"interface", "*", "admin-state"},
			},
			want:  "interface",
			want1: ".*,admin-state",
		},
		{
			name: "path_with_wildcard_2",
			args: args{
				path: []string{"interface", "mgmt0", "subinterface", "*", "admin-state"},
			},
			want:  "interface,mgmt0,subinterface",
			want1: ".*,admin-state",
		},
		{
			name: "path_with_2wildcard",
			args: args{
				path: []string{"interface", "*", "subinterface", "*", "admin-state"},
			},
			want:  "interface",
			want1: ".*,subinterface,.*,admin-state",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, _ := pathToPrefixPattern(tt.args.path)
			if got != tt.want {
				t.Errorf("pathToPrefixPattern() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("pathToPrefixPattern() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
