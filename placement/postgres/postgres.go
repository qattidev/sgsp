// Package postgres provides the durable AssignmentStore adapter. Applications
// own driver setup and invoke ApplyMigrations explicitly; New never mutates a
// database.
package postgres

import (
	"context"
	"database/sql"
	"errors"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, sgsp.ErrInvalidArgument
	}
	return &Store{db: db}, nil
}
func validGroup(group placement.GroupID) bool {
	return group.App.ID != "" && group.App.Version != "" && group.Key != ""
}
func validOwner(owner sgsp.Owner) bool {
	return owner.ID != "" && owner.Endpoint.Address != "" && owner.Endpoint.ServerName != ""
}

// Assign uses INSERT .. ON CONFLICT DO NOTHING followed by a separate SELECT
// in a READ COMMITTED transaction. Keeping these as two statements is
// essential: a one-statement CTE can miss a concurrent conflict winner.
func (s *Store) Assign(ctx context.Context, group placement.GroupID, candidate sgsp.Owner) (placement.Assignment, error) {
	if s == nil || !validGroup(group) || !validOwner(candidate) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return placement.Assignment{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO sgsp_group_assignments (app_id, group_key, app_version, owner_id, incarnation, endpoint_address, endpoint_server_name) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (app_id, group_key) DO NOTHING`, group.App.ID, group.Key, group.App.Version, candidate.ID, candidate.Incarnation[:], candidate.Endpoint.Address, candidate.Endpoint.ServerName)
	if err != nil {
		return placement.Assignment{}, err
	}
	assignment, err := readAssignment(tx.QueryRowContext(ctx, `SELECT app_version, owner_id, incarnation, endpoint_address, endpoint_server_name, closed FROM sgsp_group_assignments WHERE app_id=$1 AND group_key=$2`, group.App.ID, group.Key), group)
	if err != nil {
		return placement.Assignment{}, err
	}
	if assignment.Group.App.Version != group.App.Version {
		return placement.Assignment{}, sgsp.ErrUnsupportedVersion
	}
	if assignment.Closed {
		return placement.Assignment{}, sgsp.ErrGroupClosed
	}
	if err := tx.Commit(); err != nil {
		return placement.Assignment{}, err
	}
	return assignment, nil
}
func (s *Store) Get(ctx context.Context, group placement.GroupID) (placement.Assignment, error) {
	if s == nil || !validGroup(group) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	assignment, err := readAssignment(s.db.QueryRowContext(ctx, `SELECT app_version, owner_id, incarnation, endpoint_address, endpoint_server_name, closed FROM sgsp_group_assignments WHERE app_id=$1 AND group_key=$2`, group.App.ID, group.Key), group)
	if err != nil {
		return placement.Assignment{}, err
	}
	if assignment.Group.App.Version != group.App.Version {
		return placement.Assignment{}, sgsp.ErrUnsupportedVersion
	}
	return assignment, nil
}
func (s *Store) Close(ctx context.Context, group placement.GroupID, expected sgsp.Owner) error {
	if s == nil || !validGroup(group) || !validOwner(expected) {
		return sgsp.ErrInvalidArgument
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	assignment, err := readAssignment(tx.QueryRowContext(ctx, `SELECT app_version, owner_id, incarnation, endpoint_address, endpoint_server_name, closed FROM sgsp_group_assignments WHERE app_id=$1 AND group_key=$2 FOR UPDATE`, group.App.ID, group.Key), group)
	if err != nil {
		return err
	}
	if assignment.Group.App.Version != group.App.Version {
		return sgsp.ErrUnsupportedVersion
	}
	if assignment.Owner.ID != expected.ID || assignment.Owner.Incarnation != expected.Incarnation {
		return sgsp.ErrForbidden
	}
	if !assignment.Closed {
		if _, err := tx.ExecContext(ctx, `UPDATE sgsp_group_assignments SET closed=true, closed_at=now() WHERE app_id=$1 AND group_key=$2`, group.App.ID, group.Key); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type scanner interface{ Scan(...any) error }

func readAssignment(row scanner, group placement.GroupID) (placement.Assignment, error) {
	var version, ownerID, address, serverName string
	var incarnation []byte
	var closed bool
	if err := row.Scan(&version, &ownerID, &incarnation, &address, &serverName, &closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return placement.Assignment{}, placement.ErrAssignmentNotFound
		}
		return placement.Assignment{}, err
	}
	if len(incarnation) != len(sgsp.Incarnation{}) {
		return placement.Assignment{}, sgsp.ErrProtocolViolation
	}
	var id sgsp.Incarnation
	copy(id[:], incarnation)
	return placement.Assignment{Group: placement.GroupID{App: sgsp.AppIdentity{ID: group.App.ID, Version: version}, Key: group.Key}, Owner: sgsp.Owner{ID: ownerID, Incarnation: id, Endpoint: sgsp.Endpoint{Address: address, ServerName: serverName}}, Closed: closed}, nil
}
